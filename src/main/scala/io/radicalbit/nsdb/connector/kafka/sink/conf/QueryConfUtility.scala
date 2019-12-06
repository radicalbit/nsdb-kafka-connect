/*
 * Copyright 2019 Radicalbit S.r.l.
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

package io.radicalbit.nsdb.connector.kafka.sink.conf

import com.datamountaineer.kcql.Kcql
import io.radicalbit.nsdb.connector.kafka.sink.NSDbSinkWriter.validateDefaultValue
import io.radicalbit.nsdb.connector.kafka.sink.conf.Constants.{KcqlType, QueryType, TransformType}
import io.radicalbit.nsdb.connector.kafka.sink.models.{IQuery, ParsedKcql, QueryTransform, Transform}
import org.apache.kafka.common.config.ConfigValue

import scala.util.Try

object SupportStructure {
  val empty: SupportStructure =
    SupportStructure(List.empty[String], List.empty[String], List.empty[String], List.empty[String])
}

final case class SupportStructure(metrics: List[String],
                                  values: List[String],
                                  timestamps: List[String],
                                  tags: List[String])

trait QueryConfUtility {
  import io.circe.generic.auto._
  import io.circe.parser._
  import io.circe.syntax._

  // consider that at this point each fields format is validated
  private def parseTransformFields(metrics: String, values: String, timestamps: String, tags: String) = {

    val metricsFieldName    = "METRICS"
    val valuesFieldName     = "VALUES"
    val timestampsFieldName = "TIMESTAMPS"
    val tagsFieldName       = "TAGS"

    //we should use a parser combinator here
    def singleFieldParsing(f: String, fieldName: String): Map[String, (String, List[String])] = {
      if (f.isEmpty) Map.empty[String, (String, List[String])]
      else {
        f.split(",")
          .toList
          .flatMap(_.split('.').toList match {
            case Nil                   => None
            case head :: middle :: Nil => Some(head -> middle)
          })
          .groupBy {
            case (topic, _) => topic
          }
          .map {
            case (topic, grouped) => topic -> (fieldName, grouped.map(_._2))
          }
      }
    }

    val transforms =
      (singleFieldParsing(metrics, metricsFieldName).toSeq ++
        singleFieldParsing(values, valuesFieldName).toSeq ++
        singleFieldParsing(timestamps, timestampsFieldName).toSeq ++
        singleFieldParsing(tags, tagsFieldName).toSeq).groupBy(_._1).map {
        case (topic, fields) =>
          val SupportStructure(metricsValue, valuesValue, timestampsValue, tagsValue) =
            fields
              .map {
                case (_, grouped) => grouped
              }
              .foldRight(SupportStructure.empty) {
                case ((fieldName, list), supportStructure) =>
                  fieldName match {
                    case `metricsFieldName` => supportStructure.copy(metrics = supportStructure.metrics ++ list)
                    case `valuesFieldName`  => supportStructure.copy(values = supportStructure.values ++ list)
                    case `timestampsFieldName` =>
                      supportStructure.copy(timestamps = supportStructure.timestamps ++ list)
                    case `tagsFieldName` => supportStructure.copy(tags = supportStructure.tags ++ list)
                  }
              }

          Transform(
            topic = topic,
            metricFieldName =
              metricsValue.headOption.getOrElse(throw new IllegalArgumentException(s"Metric field for topic $topic must be defined")),
            valueFieldName = valuesValue.headOption,
            timestampFieldName = timestampsValue.headOption,
            tagsFieldName = tagsValue
          ).asJson.noSpaces
      }
    transforms
  }

  def validateAndParseQueryFormatConfig(configs: Map[String, ConfigValue]): (QueryType, Iterable[String]) = {

    val kcqlConfig       = Try(configs(NSDbConfigs.NSDB_KCQL).value().toString).toOption
    val metricsConfig    = Try(configs(NSDbConfigs.NSDB_TRANSFORM_METRICS).value().toString).toOption
    val valuesConfig     = Try(configs(NSDbConfigs.NSDB_TRANSFORM_VALUES).value().toString).toOption
    val timestampsConfig = Try(configs(NSDbConfigs.NSDB_TRANSFORM_TIMESTAMPS).value().toString).toOption
    val tagsConfig       = Try(configs(NSDbConfigs.NSDB_TRANSFORM_TAGS).value().toString).toOption

    val (queryType, queries) =
      (kcqlConfig, metricsConfig) match {
        case (Some(kcqlValue), _) =>
          (Constants.KcqlType, kcqlValue.split(";").toList)
        case (None, Some(metricsValue)) =>
          val transforms =
            parseTransformFields(metricsValue,
                                 valuesConfig.getOrElse(""),
                                 timestampsConfig.getOrElse(""),
                                 tagsConfig.getOrElse(""))
          (Constants.TransformType, transforms.toList)

        case _ =>
          throw new IllegalArgumentException("Either kcql config or transform config have not been correctly set.")
      }

    (queryType, queries)
  }

  def function(props: java.util.Map[String, String]): Map[String, Array[IQuery]] = {

    val queryType  = props.get(NSDbConfigs.NSDB_INNER_ENCODED_QUERIES_TYPE)
    val queryValue = props.get(NSDbConfigs.NSDB_INNER_ENCODED_QUERIES_VALUE)

    val globalDb        = Option(props.get(NSDbConfigs.NSDB_DB))
    val globalNamespace = Option(props.get(NSDbConfigs.NSDB_NAMESPACE))
    val defaultValue    = validateDefaultValue(Option(props.get(NSDbConfigs.NSDB_DEFAULT_VALUE)))

    QueryType.parse(queryType) match {
      case Some(KcqlType) =>
        val kcqls = queryValue.split(";").map(Kcql.parse).groupBy(_.getSource)

        val parsedKcql: Map[String, Array[IQuery]] = kcqls.map {
          case (topic, rawKcqlArray) =>
            (topic, rawKcqlArray.map(kcql => ParsedKcql(kcql, globalDb, globalNamespace, defaultValue)))
        }
        parsedKcql
      case Some(TransformType) =>
        queryValue
          .split(";")
          .map { transformString =>
            decode[Transform](transformString) match {
              case Right(tr) =>
                tr.topic -> QueryTransform(tr, globalDb, globalNamespace, defaultValue)
              case Left(err) =>
                throw new RuntimeException(s"Decoding operation failed with the following message: $err")
            }
          }
          .groupBy(_._1)
          .map {
            case (str, tuples) => (str, tuples.map(_._2))
          }
      case None =>
        throw new IllegalArgumentException(
          s"Illegal Argument $queryType for prop ${NSDbConfigs.NSDB_INNER_ENCODED_QUERIES_TYPE}")
    }
  }
}
