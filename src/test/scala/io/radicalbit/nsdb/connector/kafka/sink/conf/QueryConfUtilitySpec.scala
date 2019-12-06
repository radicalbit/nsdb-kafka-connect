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

import io.radicalbit.nsdb.connector.kafka.sink.models.{ParsedKcql, QueryTransform, Transform}
import org.scalatest.{FlatSpec, Matchers}
import io.circe.generic.auto._
import io.circe.syntax._

import scala.collection.JavaConverters._

class QueryConfUtilitySpec extends FlatSpec with Matchers with QueryConfUtility {

  private val conf = NSDbConfigs.configDef

  private def validateQueryConfTest(props: Map[String, String]) =
    validateAndParseQueryFormatConfig(conf.validate(props.asJava).asScala.map(c => (c.name(), c)).toMap)

  "QueryConfUtility" should "validate and parse kcql configuration" in {
    val kcqlQuery =
      "INSERT INTO bitC SELECT d as db, n as namespace, x AS a, y AS value, z as b, t as c FROM topicC WITHTIMESTAMP sys_time() WITHTAG(a,b)"
    val props = Map[String, String](NSDbConfigs.NSDB_KCQL -> kcqlQuery)

    val (queryType, result) =
      validateQueryConfTest(props)

    queryType shouldBe Constants.KcqlType
    result shouldBe List(kcqlQuery)
  }

  "QueryConfUtility" should "validate and parse transform configuration (all props)" in {

    val metricFieldName    = "topicC.bitC,topicB.bitC"
    val valueFieldName     = "topicC.y,topicB.y"
    val timestampFieldName = "topicC.timestamp,topicB.timestamp"
    val tagsFieldName      = "topicC.x,topicC.z,topicB.z"

    val props = Map[String, String](
      NSDbConfigs.NSDB_TRANSFORM_METRICS    -> metricFieldName,
      NSDbConfigs.NSDB_TRANSFORM_VALUES     -> valueFieldName,
      NSDbConfigs.NSDB_TRANSFORM_TIMESTAMPS -> timestampFieldName,
      NSDbConfigs.NSDB_TRANSFORM_TAGS       -> tagsFieldName
    )

    val (queryType, result) = validateQueryConfTest(props)

    val expectedResult =
      List(
        Transform("topicC", "bitC", Some("y"), Some("timestamp"), List("x", "z")).asJson.noSpaces,
        Transform("topicB", "bitC", Some("y"), Some("timestamp"), List("z")).asJson.noSpaces
      )

    queryType shouldBe Constants.TransformType
    result shouldBe expectedResult

  }

  "QueryConfUtility" should "validate and parse transform configuration (only metrics)" in {
    val metricFieldName = "topicC.bitC"

    val props = Map[String, String](NSDbConfigs.NSDB_TRANSFORM_METRICS -> metricFieldName)

    val (queryType, result) = validateQueryConfTest(props)

    val expectedResult =
      List(Transform("topicC", "bitC", None, None, List.empty[String]).asJson.noSpaces)

    queryType shouldBe Constants.TransformType
    result shouldBe expectedResult
  }

  "QueryConfUtility" should "thrown an Exception whether the configuration is wrongly set" in {
    val valueFieldName = "topicC.y,topicB.y"
    val props          = Map[String, String](NSDbConfigs.NSDB_TRANSFORM_VALUES -> valueFieldName)

    an[IllegalArgumentException] shouldBe thrownBy(validateQueryConfTest(props))
  }

  "QueryConfUtility" should "thrown an IllegalArgumentException whether metric for a configuration is not set" in {
    val metricFieldName    = "topicC.bitC"
    val valueFieldName     = "topicC.y,topicB.y"
    val timestampFieldName = "topicC.timestamp,topicB.timestamp"
    val tagsFieldName      = "topicC.x,topicC.z,topicB.z"

    val props = Map[String, String](
      NSDbConfigs.NSDB_TRANSFORM_METRICS    -> metricFieldName,
      NSDbConfigs.NSDB_TRANSFORM_VALUES     -> valueFieldName,
      NSDbConfigs.NSDB_TRANSFORM_TIMESTAMPS -> timestampFieldName,
      NSDbConfigs.NSDB_TRANSFORM_TAGS       -> tagsFieldName
    )

    an[IllegalArgumentException] shouldBe thrownBy(validateQueryConfTest(props))
  }

  "QueryConfUtility" should "function 1" in {

    val kcqlQuery =
      "INSERT INTO bitC SELECT d as db, n as namespace, x AS a, y AS value, z as b, t as c FROM topicC WITHTIMESTAMP sys_time() WITHTAG(a,b)"

    val props = Map[String, String](NSDbConfigs.NSDB_INNER_ENCODED_QUERIES_TYPE -> Constants.KcqlType.value,
                                    NSDbConfigs.NSDB_INNER_ENCODED_QUERIES_VALUE -> kcqlQuery)

    val result = function(props.asJava)
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

  "QueryConfUtility" should "function 2" in {

    val kcqlQuery =
      "INSERT INTO bitC SELECT d as db, n as namespace, x AS a, y AS value, z as b, t as c FROM topicC WITHTIMESTAMP sys_time() WITHTAG(a,b)"

    val props = Map[String, String](NSDbConfigs.NSDB_INNER_ENCODED_QUERIES_TYPE -> "wrong-value",
                                    NSDbConfigs.NSDB_INNER_ENCODED_QUERIES_VALUE -> kcqlQuery)

    an[IllegalArgumentException] shouldBe thrownBy(function(props.asJava))
  }

  "QueryConfUtility" should "function 3" in {

    val transform =
      Transform("topicC", "bitC", Some("y"), Some("timestamp"), List("x", "z")).asJson.noSpaces

    val props = Map[String, String](
      NSDbConfigs.NSDB_INNER_ENCODED_QUERIES_TYPE  -> Constants.TransformType.value,
      NSDbConfigs.NSDB_INNER_ENCODED_QUERIES_VALUE -> transform,
      NSDbConfigs.NSDB_DB                          -> "db",
      NSDbConfigs.NSDB_NAMESPACE                   -> "namespace"
    )

    val result = function(props.asJava)

    result.keys shouldBe Set("topicC")
    result.values.toList.flatten shouldBe List(
      QueryTransform(dbField = "db",
        namespaceField = "namespace",
        metric = "bitC",
        defaultValue = None,
        timestampField = Some("timestamp"),
        valueField = Some("y"),
        tagsNames = List("x", "z"))
    )
  }

}
