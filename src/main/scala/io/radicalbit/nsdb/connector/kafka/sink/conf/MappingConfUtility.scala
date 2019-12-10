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
import io.radicalbit.nsdb.connector.kafka.sink.conf.Constants.{KcqlType, MapType, MappingType}
import io.radicalbit.nsdb.connector.kafka.sink.models.{MappingInterface, ParsedKcql, EnrichedMapping, Mappings}
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

trait MappingConfUtility {
  import io.circe.generic.auto._
  import io.circe.parser._
  import io.circe.syntax._

  // consider that at this point each fields format is validated
  private def parseMappings(metrics: String,
                            values: Option[String],
                            timestamps: Option[String],
                            tags: Option[String]) = {

    val metricsFieldName    = "METRICS"
    val valuesFieldName     = "VALUES"
    val timestampsFieldName = "TIMESTAMPS"
    val tagsFieldName       = "TAGS"

    val dottedNotationToken = "[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+".r

    //we should use a parser combinator here
    def tokenize(optF: Option[String], fieldName: String): Map[String, (String, List[String])] =
      optF
        .map { f =>
          dottedNotationToken
            .findAllMatchIn(f)
            .toList
            .flatMap(_.toString().split('.').toList match { // safe because of dotted notation validation
              case Nil                   => None
              case head :: middle :: Nil => Some(head -> middle)
            })
            .groupBy {
              case (topic, _) => topic
            }
            .map {
              case (topic, grouped) =>
                topic -> (fieldName, grouped.map {
                  case (_, parsedValue) => parsedValue
                })
            }
        }
        .getOrElse(Map.empty[String, (String, List[String])])

    val mappings =
      (tokenize(Some(metrics), metricsFieldName).toSeq ++
        tokenize(values, valuesFieldName).toSeq ++
        tokenize(timestamps, timestampsFieldName).toSeq ++
        tokenize(tags, tagsFieldName).toSeq)
        .groupBy {
          case (topic, _) => topic
        }
        .map {
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

            Mappings(
              topic = topic,
              metricFieldName = metricsValue.headOption.getOrElse(
                throw new IllegalArgumentException(s"Metric field for topic $topic must be defined")),
              valueFieldName = valuesValue.headOption,
              timestampFieldName = timestampsValue.headOption,
              tagsFieldName = tagsValue
            ).asJson.noSpaces
        }
    mappings
  }

  /**
    * Checks if the query configuration (either kcql or transform query) has been correctly set
    * Parse then the value of either kcql or mappings
    *
    * @param configs Properties configuration of connector
    * @return [[MapType]] along with List of mappings interface object as string
    */
  def validateAndParseMappingsConfig(configs: Map[String, ConfigValue]): (MapType, List[String]) = {

    val kcqlConfig       = Try(configs(NSDbConfigs.NSDB_KCQL).value().toString).toOption
    val metricsConfig    = Try(configs(NSDbConfigs.NSDB_MAPPING_METRICS).value().toString).toOption
    val valuesConfig     = Try(configs(NSDbConfigs.NSDB_MAPPING_VALUES).value().toString).toOption
    val timestampsConfig = Try(configs(NSDbConfigs.NSDB_MAPPING_TIMESTAMPS).value().toString).toOption
    val tagsConfig       = Try(configs(NSDbConfigs.NSDB_MAPPING_TAGS).value().toString).toOption

    val (queryType, mappingsInterfaceAsString) =
      (kcqlConfig, metricsConfig) match {
        case (Some(kcqlValue), _) =>
          (Constants.KcqlType, kcqlValue.split(";").toList)
        case (None, Some(metricsValue)) =>
          val mappingsByTopic =
            parseMappings(
              metricsValue,
              valuesConfig,
              timestampsConfig,
              tagsConfig
            )
          (Constants.MappingType, mappingsByTopic.toList)

        case _ =>
          throw new IllegalArgumentException("Either kcql config or mapping config have not been correctly set.")
      }

    (queryType, mappingsInterfaceAsString)
  }

  /**
    * From string to MappingInterface objects
    * @param props Properties configuration of a task
    * @return Topic along with [[MappingInterface]] implementations as map
    */
  def mapToStringToMappingInterfaces(props: java.util.Map[String, String]): Map[String, Array[MappingInterface]] = {

    val mappingsType  = props.get(NSDbConfigs.NSDB_ENCODED_MAPPINGS_TYPE)
    val mappingsValue = props.get(NSDbConfigs.NSDB_ENCODED_MAPPINGS_VALUE)

    val globalDb        = Option(props.get(NSDbConfigs.NSDB_DB))
    val globalNamespace = Option(props.get(NSDbConfigs.NSDB_NAMESPACE))
    val defaultValue    = validateDefaultValue(Option(props.get(NSDbConfigs.NSDB_DEFAULT_VALUE)))

    MapType.parse(mappingsType) match {
      case Some(KcqlType) =>
        val kcqls = mappingsValue.split(";").map(Kcql.parse).groupBy(_.getSource)

        val parsedKcql: Map[String, Array[MappingInterface]] = kcqls.map {
          case (topic, rawKcqlArray) =>
            (topic, rawKcqlArray.map(kcql => ParsedKcql(kcql, globalDb, globalNamespace, defaultValue)))
        }
        parsedKcql
      case Some(MappingType) =>
        mappingsValue
          .split(";")
          .map { mappingString =>
            decode[Mappings](mappingString) match {
              case Right(mappings) =>
                mappings.topic -> EnrichedMapping(mappings, globalDb, globalNamespace, defaultValue)
              case Left(err) =>
                throw new RuntimeException(s"Decoding operation failed with the following message: $err")
            }
          }
          .groupBy {
            case (topic, _) => topic
          }
          .map {
            case (topic, tuples) =>
              (topic, tuples.map {
                case (_, interface) => interface
              })
          }
      case None =>
        throw new IllegalArgumentException(
          s"Illegal Argument $mappingsType for prop ${NSDbConfigs.NSDB_ENCODED_MAPPINGS_TYPE}")
    }
  }
}
