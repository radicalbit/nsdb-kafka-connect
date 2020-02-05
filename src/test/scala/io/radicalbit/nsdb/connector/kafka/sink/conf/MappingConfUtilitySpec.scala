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

package io.radicalbit.nsdb.connector.kafka.sink.conf

import io.radicalbit.nsdb.connector.kafka.sink.models.{EnrichedMapping, Mappings, ParsedKcql}
import org.scalatest.{FlatSpec, Matchers}
import io.circe.generic.auto._
import io.circe.syntax._
import org.apache.kafka.common.config.ConfigException

import scala.collection.JavaConverters._

class MappingConfUtilitySpec extends FlatSpec with Matchers with MappingConfUtility {

  private val conf = NSDbConfigs.configDef

  private def validateQueryConfTest(props: Map[String, String]) =
    validateAndParseMappingsConfig(conf.validate(props.asJava).asScala.map(c => (c.name(), c)).toMap)

  "QueryConfUtility" should "validate and parse kcql configuration" in {
    val kcqlQuery =
      "INSERT INTO bitC SELECT d as db, n as namespace, x AS a, y AS value, z as b, t as c FROM topicC WITHTIMESTAMP sys_time() WITHTAG(a,b)"
    val props = Map[String, String](NSDbConfigs.NSDB_KCQL -> kcqlQuery)

    val (queryType, result) =
      validateQueryConfTest(props)

    queryType shouldBe Constants.KcqlType
    result shouldBe List(kcqlQuery)
  }

  "QueryConfUtility" should "validate and parse mappings configuration (all props)" in {

    val metricFieldName    = "topicC.bitC,topicB.bitC"
    val valueFieldName     = "topicC.y,topicB.y"
    val timestampFieldName = "topicC.timestamp,topicB.timestamp"
    val tagsFieldName      = "topicC.x,topicC.z,topicB.z"

    val props = Map[String, String](
      NSDbConfigs.NSDB_MAPPING_METRICS    -> metricFieldName,
      NSDbConfigs.NSDB_MAPPING_VALUES     -> valueFieldName,
      NSDbConfigs.NSDB_MAPPING_TIMESTAMPS -> timestampFieldName,
      NSDbConfigs.NSDB_MAPPING_TAGS       -> tagsFieldName
    )

    val (queryType, result) = validateQueryConfTest(props)

    val expectedResult =
      List(
        Mappings("topicC", "bitC", Some("y"), Some("timestamp"), List("x", "z")).asJson.noSpaces,
        Mappings("topicB", "bitC", Some("y"), Some("timestamp"), List("z")).asJson.noSpaces
      )

    queryType shouldBe Constants.MappingType
    result shouldBe expectedResult

  }

  "QueryConfUtility" should "validate and parse mappings configuration (only metrics)" in {
    val metricFieldName = "topicC.bitC"

    val props = Map[String, String](NSDbConfigs.NSDB_MAPPING_METRICS -> metricFieldName)

    val (queryType, result) = validateQueryConfTest(props)

    val expectedResult =
      List(Mappings("topicC", "bitC", None, None, List.empty[String]).asJson.noSpaces)

    queryType shouldBe Constants.MappingType
    result shouldBe expectedResult
  }

  "QueryConfUtility" should "thrown an Exception whether the configuration is wrongly set" in {
    val valueFieldName = "topicC.y,topicB.y"
    val props          = Map[String, String](NSDbConfigs.NSDB_MAPPING_VALUES -> valueFieldName)

    an[ConfigException] shouldBe thrownBy(validateQueryConfTest(props))
  }

  "QueryConfUtility" should "thrown an ConfigException whether metric for a configuration is not set" in {
    val metricFieldName    = "topicC.bitC"
    val valueFieldName     = "topicC.y,topicB.y"
    val timestampFieldName = "topicC.timestamp,topicB.timestamp"
    val tagsFieldName      = "topicC.x,topicC.z,topicB.z"

    val props = Map[String, String](
      NSDbConfigs.NSDB_MAPPING_METRICS    -> metricFieldName,
      NSDbConfigs.NSDB_MAPPING_VALUES     -> valueFieldName,
      NSDbConfigs.NSDB_MAPPING_TIMESTAMPS -> timestampFieldName,
      NSDbConfigs.NSDB_MAPPING_TAGS       -> tagsFieldName
    )

    an[ConfigException] shouldBe thrownBy(validateQueryConfTest(props))
  }

  "QueryConfUtility" should "correctly parses parsed kcql object" in {

    val kcqlQuery =
      "INSERT INTO bitC SELECT d as db, n as namespace, x AS a, y AS value, z as b, t as c FROM topicC WITHTIMESTAMP sys_time() WITHTAG(a,b)"

    val props = Map[String, String](NSDbConfigs.NSDB_ENCODED_MAPPINGS_TYPE -> Constants.KcqlType.value,
                                    NSDbConfigs.NSDB_ENCODED_MAPPINGS_VALUE -> kcqlQuery)

    val result = mapToStringToMappingInterfaces(props.asJava)
    result.keys shouldBe Set("topicC")
    result.values.toList.flatten shouldBe List(
      ParsedKcql(
        dbField = "d",
        namespaceField = "n",
        metric = "bitC",
        defaultValue = None,
        timestampField = Some("sys_time()"),
        valueField = Some("y"),
        tagAliasesMap = Map("a"       -> "x", "b" -> "z"),
        dimensionAliasesMap = Map("c" -> "t")
      ))
  }

  "QueryConfUtility" should "throws an ConfigException whether nsdb.inner.encoded.queries.type prop is wrongly set" in {

    val kcqlQuery =
      "INSERT INTO bitC SELECT d as db, n as namespace, x AS a, y AS value, z as b, t as c FROM topicC WITHTIMESTAMP sys_time() WITHTAG(a,b)"

    val props = Map[String, String](NSDbConfigs.NSDB_ENCODED_MAPPINGS_TYPE -> "wrong-value",
                                    NSDbConfigs.NSDB_ENCODED_MAPPINGS_VALUE -> kcqlQuery)

    an[ConfigException] shouldBe thrownBy(mapToStringToMappingInterfaces(props.asJava))
  }

  "QueryConfUtility" should "correctly parses enriched mapping object" in {

    val mappings =
      Mappings("topicC", "bitC", Some("y"), Some("timestamp"), List("x", "z")).asJson.noSpaces

    val props = Map[String, String](
      NSDbConfigs.NSDB_ENCODED_MAPPINGS_TYPE  -> Constants.MappingType.value,
      NSDbConfigs.NSDB_ENCODED_MAPPINGS_VALUE -> mappings,
      NSDbConfigs.NSDB_DB                     -> "db",
      NSDbConfigs.NSDB_NAMESPACE              -> "namespace"
    )

    val result = mapToStringToMappingInterfaces(props.asJava)

    result.keys shouldBe Set("topicC")
    result.values.toList.flatten shouldBe List(
      EnrichedMapping(dbField = "db",
                      namespaceField = "namespace",
                      metric = "bitC",
                      defaultValue = None,
                      timestampField = Some("timestamp"),
                      valueField = Some("y"),
                      tagsNames = List("x", "z"))
    )
  }

}
