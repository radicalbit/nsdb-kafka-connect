/*
 * Copyright 2019-2020 Radicalbit S.r.l.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.radicalbit.nsdb.connector.kafka.sink

import cats.Id
import com.typesafe.scalalogging.{Logger, StrictLogging}
import io.radicalbit.nsdb.api.scala.{Bit, NSDB}
import io.radicalbit.nsdb.connector.kafka.sink.conf.Constants._
import io.radicalbit.nsdb.connector.kafka.sink.models.MappingInterface
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import org.apache.kafka.connect.data.Schema.Type
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.sink.SinkRecord
import org.slf4j.LoggerFactory
import retry._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.duration._
import scala.concurrent.{Await, Future}
import scala.util.{Failure, Success, Try}

/**
  * Handles writes to NSDb.
  */
class NSDbSinkWriter(connection: NSDB,
                     stringToMappingInterfaces: Map[String, Array[MappingInterface]],
                     globalDb: Option[String],
                     globalNamespace: Option[String],
                     defaultValue: Option[java.math.BigDecimal],
                     retentionPolicy: Option[Duration],
                     shardInterval: Option[Duration],
                     semanticDelivery: Option[SemanticDelivery],
                     retries: Int,
                     retryInterval: Duration,
                     timeout: Duration)
    extends StrictLogging {

  logger.info("Initialising NSDb writer")

  lazy val initInfoProvided: Boolean = Seq(shardInterval, retentionPolicy).flatten.nonEmpty

  val initializedCoordinates: mutable.Map[(String, String, String), Boolean] = mutable.Map.empty

  /**
    * Write a list of SinkRecords to NSDb.
    *
    * @param records The list of SinkRecords to write.
    **/
  def write(records: List[SinkRecord]): Unit = {
    if (records.isEmpty) {
      logger.debug("No records received.")
    } else {
      logger.debug("Received {} records.", records.size)
      val grouped = records.groupBy(_.topic())
      grouped.foreach({
        case (topic, entries) =>
          writeRecords(topic,
                       entries,
                       stringToMappingInterfaces.getOrElse(topic, Array.empty),
                       globalDb,
                       globalNamespace,
                       defaultValue)
      })
    }
  }

  /**
    * Write a list of sink records to NSDb.
    *
    * @param topic   The source topic.
    * @param records The list of sink records to write.
    **/
  private def writeRecords(topic: String,
                           records: List[SinkRecord],
                           mappingInterfaces: Array[MappingInterface],
                           globalDb: Option[String],
                           globalNamespace: Option[String],
                           defaultValue: Option[java.math.BigDecimal]): Unit = {
    logger.debug("Handling {} records for topic {}. Found also {} kcql queries.",
                 records.size,
                 topic,
                 mappingInterfaces)

    import NSDbSinkWriter.{logger => _, _}

    val recordMaps = records.map(parse(_, globalDb, globalNamespace, defaultValue))

    mappingInterfaces.foreach { mappingInterface =>
      logger.debug(
        "Handling query: \t{}\n Found also user params db: {}, namespace: {}, defaultValue: {}",
        mappingInterface,
        globalDb.isDefined,
        globalNamespace.isDefined,
        defaultValue.isDefined
      )
      val bitSeq: Future[List[Bit]] = Future.sequence(recordMaps.map(map => {
        val convertedBit = mappingInterface.convertToBit(map)

        if (initInfoProvided)
          initializedCoordinates.get((convertedBit.db, convertedBit.namespace, convertedBit.metric)) match {
            case Some(_) => Future(convertedBit)
            case None =>
              initializedCoordinates += (convertedBit.db, convertedBit.namespace, convertedBit.metric) -> true
              connection
                .init(
                  connection
                    .db(convertedBit.db)
                    .namespace(convertedBit.namespace)
                    .metric(convertedBit.metric)
                    .shardInterval(shardInterval.map(_.toString).getOrElse("0d"))
                    .retention(retentionPolicy.map(_.toString).getOrElse("0d")))
                .map { response =>
                  if (!response.completedSuccessfully)
                    logger.warn(
                      "init metric for db: {}, namespace: {}, metric: {} completed with error {}",
                      convertedBit.db,
                      convertedBit.namespace,
                      convertedBit.metric,
                      response.errorMsg
                    )
                  convertedBit
                }
          } else
          Future(convertedBit)
      }))

      writeWithDeliveryPolicy(semanticDelivery, bitSeq.flatMap(connection.write), retries, retryInterval, timeout)

      logger.debug("Wrote {} to NSDb.", recordMaps.length)
    }

  }

  def close(): Unit = connection.close()
}

object NSDbSinkWriter {

  private val logger = Logger(LoggerFactory.getLogger(classOf[NSDbSinkWriter]))

  private def getFieldName(parent: Option[String], field: String) = parent.map(p => s"$p.$field").getOrElse(field)

  private case class WriteFailureException(msg: String) extends RuntimeException(msg)

  /**
    * Manipulate future result according to semantic delivery policy
    * @param semanticDelivery [[SemanticDelivery]] if not defined, at_most_once delivery is chosen
    * @param fResult future result of nsdb grpc write
    * @param maxRetries maximum number of retries if at_least_once semantic delivery is chosen
    * @param retryInterval time interval between two retries if at least once semantic delivery is chosen
    * @param timeout time to wait for future completing if at least once semantic delivery is chosen
    */
  def writeWithDeliveryPolicy(semanticDelivery: Option[SemanticDelivery],
                              fResult: => Future[List[RPCInsertResult]],
                              maxRetries: Int,
                              retryInterval: Duration,
                              timeout: Duration) = {

    val policy = RetryPolicies.limitRetries[Id](maxRetries - 1)

    val finiteDurationSleep: FiniteDuration = FiniteDuration(retryInterval._1, retryInterval._2)

    def wasSuccessful: Try[List[RPCInsertResult]] => Boolean =
      t => t.map(_.forall(_.completedSuccessfully)).getOrElse(false)

    def onFailure(failure: Try[List[RPCInsertResult]], details: RetryDetails): Unit = {
      failure match {
        case Success(rpcResultsWithFailures) =>
          rpcResultsWithFailures.foreach {
            case RPCInsertResult(false, error) =>
              logger.warn(s"an error occurred during sink: $error. Retrying...")
            case _ => //do nothing
          }
        case Failure(exception) => logger.warn(s"an error occurred during sink. Retrying...", exception)
      }
      Sleep[Id].sleep(finiteDurationSleep)
    }

    semanticDelivery match {
      case Some(AtLeastOnce) =>
        retrying(policy, wasSuccessful, onFailure)(
          Try(
            Await.result(
              fResult.flatMap {
                case rPCInsertResults if !rPCInsertResults.forall(_.completedSuccessfully) =>
                  Future.failed(WriteFailureException("Field 'completedSuccessfully' returns false"))
                case rPCInsertResults =>
                  Future.successful(rPCInsertResults)
              },
              timeout
            ))
        ) match {
          case Failure(exception) => throw exception
          case Success(value)     => value
        }
      case _ =>
        fResult
    }
  }

  /**
    * Validate the semantic delivery property according to possible fixed values
    * @param configName
    * @param configValue
    */
  def validateSemanticDelivery(configName: String, configValue: String): Option[SemanticDelivery] = {
    val maybeProp = SemanticDelivery.parse(configValue)
    require(
      maybeProp.isDefined,
      s"""value $configValue for $configName is not valid. Possible values are: ${SemanticDelivery.possibleValues
        .mkString(", ")}"""
    )
    maybeProp
  }

  /**
    * this custom validation has put here because kafka connect does not allow to specify more than one type at once.
    * Hence, a string type has been chosen and it is checked if it is a valid BigDecimal or not.
    */
  def validateDefaultValue(defaultValueStr: Option[String]): Option[java.math.BigDecimal] = {
    require(Try(new java.math.BigDecimal(defaultValueStr.getOrElse("0"))).isSuccess,
            s"value $defaultValueStr as default value is invalid, must be a number")
    defaultValueStr.map(new java.math.BigDecimal(_))
  }

  /**
    * Validate if a provided string is a duration or not.
    * @param configName the name of the config.
    * @param configValue the optional string value.
    */
  def validateDuration(configName: String, configValue: Option[String]): Option[Duration] = {
    require(Try(configValue.map(Duration.apply)).isSuccess,
            s"value $configValue for $configName is not a valid duration")
    configValue.map(Duration.apply)
  }

  /**
    * Recursively build a Map to represent a field.
    *
    * @param field  The field schema to add.
    * @param struct The struct to extract the value from.
    **/
  private def buildField(field: Field,
                         struct: Struct,
                         parentField: Option[String] = None,
                         acc: Map[String, Any] = Map.empty): Map[String, Any] = {
    logger.debug("Parsing field {}. Parent field availability is {}.", field.name, parentField.isDefined)
    val value = struct.get(field)

    val outcome = (field.schema.`type`, field.schema.name, value) match {
      case (_, _, nullValue) if Option(nullValue).isEmpty => Nil
      case (Type.STRUCT, _, _) =>
        logger.debug("Field {} is a Struct. Calling self recursively.", field.name)
        val nested = struct.getStruct(field.name)
        val schema = nested.schema
        val fields = schema.fields.asScala
        fields.flatMap(f => buildField(f, nested, Some(field.name)))

      case (Type.BYTES, Decimal.LOGICAL_NAME, decimalValue: java.math.BigDecimal) =>
        logger.debug("Field {} is Bytes and is a Decimal.", field.name)
        getFieldName(parentField, field.name) -> decimalValue :: Nil

      case (Type.BYTES, Decimal.LOGICAL_NAME, _) =>
        logger.error("Field {} is {} but its value type is unknown. Raising unsupported exception.",
                     field.name,
                     Decimal.LOGICAL_NAME)
        sys.error(s"Found logical Decimal type but value $value has unknown type ${Option(value).map(_.getClass)}.")

      case (Type.BYTES, _, _) =>
        logger.debug("Field {} is {} and is a {}.", field.name, Type.BYTES, Decimal.LOGICAL_NAME)
        val str = new String(struct.getBytes(field.name), "utf-8")
        getFieldName(parentField, field.name) -> str :: Nil

      case (typ, Time.LOGICAL_NAME, dateValue: java.util.Date) =>
        logger.debug("Field {} is {} and is a {}.", field.name, typ, Time.LOGICAL_NAME)
        getFieldName(parentField, field.name) -> dateValue.getTime :: Nil

      case (_, Time.LOGICAL_NAME, _) =>
        logger.error("Field {} is {} but its value type is unknown. Raising unsupported exception.",
                     field.name,
                     Time.LOGICAL_NAME)
        sys.error(
          s"Found logical ${Time.LOGICAL_NAME} type but value has unknown type ${Option(value).map(_.getClass)}.")

      case (typ, Timestamp.LOGICAL_NAME, dateValue: java.util.Date) =>
        logger.debug("Field {} is {} and is a {}.", field.name, typ, Timestamp.LOGICAL_NAME)
        getFieldName(parentField, field.name) -> dateValue.getTime :: Nil

      case (_, Timestamp.LOGICAL_NAME, _) =>
        logger.error("Field {} is {} but its value type is unknown. Raising unsupported exception.",
                     field.name,
                     Timestamp.LOGICAL_NAME)
        sys.error(
          s"Found logical ${Timestamp.LOGICAL_NAME} type but value has unknown type ${Option(value).map(_.getClass)}.")

      case (typ, Date.LOGICAL_NAME, dateValue: java.util.Date) =>
        logger.debug("Field {} is {} and is a {}.", field.name, typ, Date.LOGICAL_NAME)
        getFieldName(parentField, field.name) -> dateValue.getTime :: Nil

      case (_, Date.LOGICAL_NAME, _) =>
        logger.error("Field {} is {} but its value type is unknown. Raising unsupported exception.",
                     field.name,
                     Date.LOGICAL_NAME)
        sys.error(
          s"Found logical ${Date.LOGICAL_NAME} type but value has unknown type ${Option(value).map(_.getClass)}.")

      case (typ, logical, plainValue) =>
        logger.debug("Field {} is {} and is a {}.", field.name, typ, logical)
        getFieldName(parentField, field.name) -> plainValue :: Nil
    }

    acc ++ outcome.toMap
  }

  def parse(record: SinkRecord,
            globalDb: Option[String],
            globalNamespace: Option[String],
            defaultValue: Option[java.math.BigDecimal]): Map[String, Any] = {
    logger.debug("Parsing SinkRecord {}.", record)

    val schema = record.valueSchema
    if (schema == null) {
      logger.error("Given record {} has not Schema. Raising unsupported exception.", record)
      sys.error(s"Schemaless records are not supported. Record ${record.toString} doesn't own any schema.")
    } else {
      schema.`type` match {
        case Schema.Type.STRUCT =>
          val s      = record.value.asInstanceOf[Struct]
          val fields = schema.fields.asScala.flatMap(f => buildField(f, s))

          val globals: mutable.ListBuffer[(String, Any)] = mutable.ListBuffer.empty[(String, Any)]
          globalDb.foreach(db => globals += ((db, db)))
          globalNamespace.foreach(ns => globals += ((ns, ns)))

          (fields union globals).toMap
        case other =>
          logger.error("Given record {} was not a Struct. Raising unsupported exception.", record)
          sys.error(s"$other schema is not supported.")
      }
    }
  }
}
