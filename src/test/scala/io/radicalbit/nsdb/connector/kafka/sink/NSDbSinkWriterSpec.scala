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

import com.typesafe.scalalogging.{Logger, StrictLogging}
import io.radicalbit.nsdb.api.scala.{Bit, Db}
import io.radicalbit.nsdb.connector.kafka.sink.conf.Constants.AtLeastOnce
import io.radicalbit.nsdb.connector.kafka.sink.models.{EnrichedMapping, Mappings, ParsedKcql}
import io.radicalbit.nsdb.rpc.response.RPCInsertResult
import org.apache.kafka.connect.data._
import org.apache.kafka.connect.sink.SinkRecord
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

import scala.concurrent.Future
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import scala.util.{Success, Try}

class NSDbSinkWriterSpec extends FlatSpec with Matchers with OneInstancePerTest with StrictLogging {

  implicit val loggerImpl: Logger = logger

  val simpleSchema = SchemaBuilder.struct
    .name("record")
    .version(1)
    .field("string_id", Schema.STRING_SCHEMA)
    .field("int_field", Schema.INT32_SCHEMA)
    .field("long_field", Schema.INT64_SCHEMA)
    .field("string_field", Schema.STRING_SCHEMA)
    .build

  val simpleStruct = new Struct(simpleSchema)
    .put("string_id", "my_id_val")
    .put("int_field", 12)
    .put("long_field", 12L)
    .put("string_field", "foo")

  val simpleSchemaWithDimensions = SchemaBuilder.struct
    .name("record")
    .version(1)
    .field("string_id", Schema.STRING_SCHEMA)
    .field("int_field", Schema.INT32_SCHEMA)
    .field("long_field", Schema.INT64_SCHEMA)
    .field("string_field", Schema.STRING_SCHEMA)
    .field("d1", Schema.STRING_SCHEMA)
    .field("d2", Schema.INT32_SCHEMA)
    .build

  val simpleStructWithDimensions = new Struct(simpleSchemaWithDimensions)
    .put("string_id", "my_id_val")
    .put("int_field", 12)
    .put("long_field", 12L)
    .put("string_field", "foo")
    .put("d1", "d1")
    .put("d2", 12)

  val schema = SchemaBuilder.struct
    .name("record")
    .version(1)
    .field("string_id", Schema.STRING_SCHEMA)
    .field("int_field", Schema.INT32_SCHEMA)
    .field("long_field", Schema.INT64_SCHEMA)
    .field("string_field", Schema.STRING_SCHEMA)
    .field("my_struct", simpleSchema)
    .build

  val struct = new Struct(schema)
    .put("string_id", "my_id_val")
    .put("int_field", 12)
    .put("long_field", 12L)
    .put("string_field", "foo")
    .put("my_struct", simpleStruct)

  val optSchema = SchemaBuilder.struct
    .name("record")
    .version(1)
    .field("non_opt_field", Schema.STRING_SCHEMA)
    .field("empty_opt_field", Schema.OPTIONAL_INT32_SCHEMA)
    .field("filled_opt_field", Schema.OPTIONAL_INT32_SCHEMA)
    .build()

  val optStruct = new Struct(optSchema)
    .put("non_opt_field", "foo")
    .put("empty_opt_field", null)
    .put("filled_opt_field", 30)

  val decimalSchema = SchemaBuilder.struct
    .name("record")
    .version(1)
    .field("decimal", Decimal.schema(2))
    .field("empty_nullable_decimal", Decimal.builder(2).optional.build())
    .field("filled_nullable_decimal", Decimal.builder(2).optional.build())
    .build

  val decimalStruct = new Struct(decimalSchema)
    .put("decimal", new java.math.BigDecimal("30.44"))
    .put("empty_nullable_decimal", null)
    .put("filled_nullable_decimal", new java.math.BigDecimal("15.01"))

  val timestampSchema = SchemaBuilder.struct
    .name("record")
    .version(1)
    .field("timestamp", Timestamp.SCHEMA)
    .field("empty_nullable_timestamp", Timestamp.builder.optional.build())
    .field("filled_nullable_timestamp", Timestamp.builder.optional.build())
    .build

  val sampleDate = new java.util.Date(1528974440000L)
  val timestampStruct = new Struct(timestampSchema)
    .put("timestamp", sampleDate)
    .put("empty_nullable_timestamp", null)
    .put("filled_nullable_timestamp", sampleDate)

  val simpleRecord = new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", simpleSchema, simpleStruct, 1)
  val simpleRecordWithDimentions =
    new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", simpleSchemaWithDimensions, simpleStructWithDimensions, 1)
  val record          = new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", schema, struct, 1)
  val stringRecord    = new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", schema, "", 1)
  val optRecord       = new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", optSchema, optStruct, 1)
  val decimalRecord   = new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", decimalSchema, decimalStruct, 1)
  val timestampRecord = new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", timestampSchema, timestampStruct, 1)

  "SinkRecordConversion" should "convert a struct SinkRecord to a Map" in {
    val mo = NSDbSinkWriter.parse(simpleRecord, None, None, None)

    mo.keys.size shouldBe 4
    mo.get("string_id") shouldBe Some("my_id_val")
    mo.get("int_field") shouldBe Some(12)
    mo.get("long_field") shouldBe Some(12)
  }

  "SinkRecordConversion" should "convert a SinkRecord with nested type to a Map" in {

    val mo = NSDbSinkWriter.parse(record, None, None, None)
    mo.keys.size shouldBe 8
    mo.get("string_id") shouldBe Some("my_id_val")
    mo.get("int_field") shouldBe Some(12)
    mo.get("long_field") shouldBe Some(12)
    mo.get("string_field") shouldBe Some("foo")

    mo.get("my_struct.string_id") shouldBe Some("my_id_val")
    mo.get("my_struct.int_field") shouldBe Some(12)
    mo.get("my_struct.long_field") shouldBe Some(12)
    mo.get("my_struct.string_field") shouldBe Some("foo")

  }

  "SinkRecordConversion" should "convert a SinkRecord with nullable fields as dimension" in {

    val mo = NSDbSinkWriter.parse(optRecord, None, None, None)
    mo.keys.size shouldBe 2
    mo.get("non_opt_field") shouldBe Some("foo")
    mo.get("empty_opt_field") shouldBe None
    mo.get("filled_opt_field") shouldBe Some(30)
  }

  "SinkRecordConversion" should "convert a SinkRecord when has a big decimal as logical type" in {
    val mo = NSDbSinkWriter.parse(decimalRecord, None, None, None)
    mo.keys.size shouldBe 2

    mo.get("decimal") shouldBe Some(new java.math.BigDecimal("30.44"))
    mo.get("empty_nullable_decimal") shouldBe None
    mo.get("filled_nullable_decimal") shouldBe Some(new java.math.BigDecimal("15.01"))
  }

  "SinkRecordConversion" should "convert a SinkRecord when has a timestamp logical type" in {
    val mo = NSDbSinkWriter.parse(timestampRecord, None, None, None)
    mo.keys.size shouldBe 2

    mo.get("timestamp") shouldBe Some(sampleDate.getTime)
    mo.get("empty_nullable_timestamp") shouldBe None
    mo.get("filled_nullable_timestamp") shouldBe Some(sampleDate.getTime)
  }

  "SinkRecordConversion" should "ignore non struct records" in {
    val stringRecord = new SinkRecord("topic", 1, Schema.STRING_SCHEMA, "key", schema, "a generic string", 1)
    an[RuntimeException] should be thrownBy NSDbSinkWriter.parse(stringRecord, None, None, None)
  }

  "SinkRecordConversion" should "successfully convert records given a kcql and no global params" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_id AS db, string_field AS namespace, int_field AS value, d2, d1 FROM topic WITHTIMESTAMP long_field"

    val parsedKcql = ParsedKcql(withDimensionAlias, None, None, None)

    val bit: Bit =
      parsedKcql.convertToBit(NSDbSinkWriter.parse(simpleRecordWithDimentions, None, None, None))

    val expectedBit =
      Db("my_id_val")
        .namespace("foo")
        .metric("metric")
        .timestamp(12)
        .value(12)
        .dimension("d2", 12)
        .dimension("d1", "d1")

    bit shouldBe expectedBit
  }

  "SinkRecordConversion" should "successfully convert records given a kcql and global db" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_field AS namespace, int_field AS value FROM topic WITHTIMESTAMP long_field"

    val parsedKcql = ParsedKcql(withDimensionAlias, Some("globalDb"), None, None)

    val bit: Bit =
      parsedKcql.convertToBit(NSDbSinkWriter.parse(simpleRecordWithDimentions, Some("globalDb"), None, None))

    val expectedBit =
      Db("globalDb").namespace("foo").metric("metric").timestamp(12).value(12)

    bit shouldBe expectedBit
  }

  "SinkRecordConversion" should "successfully convert records given a kcql and global namespace" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_id AS db, int_field AS value FROM topic WITHTIMESTAMP long_field"

    val parsedKcql = ParsedKcql(withDimensionAlias, None, Some("globalNs"), None)

    val bit: Bit =
      parsedKcql.convertToBit(NSDbSinkWriter.parse(simpleRecordWithDimentions, None, Some("globalNs"), None))

    val expectedBit =
      Db("my_id_val").namespace("globalNs").metric("metric").timestamp(12).value(12)

    bit shouldBe expectedBit
  }

  "SinkRecordConversion" should "successfully convert records given a kcql and global parameters" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT int_field AS value, d1 as dim1, d2 as dim2 FROM topic WITHTIMESTAMP long_field"

    val parsedKcql = ParsedKcql(withDimensionAlias, Some("globalDb"), Some("globalNs"), None)

    val bit: Bit =
      parsedKcql.convertToBit(
        NSDbSinkWriter.parse(simpleRecordWithDimentions, Some("globalDb"), Some("globalNs"), None))

    val expectedBit =
      Db("globalDb")
        .namespace("globalNs")
        .metric("metric")
        .timestamp(12)
        .value(12)
        .dimension("dim1", "d1")
        .dimension("dim2", 12)

    bit shouldBe expectedBit
  }

  "SinkRecordConversion" should "successfully convert records given a kcql and bigDecimal dimension" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT decimal AS value, empty_nullable_decimal as d1, filled_nullable_decimal as d2 FROM topic WITHTIMESTAMP now"

    val parsedKcql = ParsedKcql(withDimensionAlias, Some("globalDb"), Some("globalNs"), None)

    val bit: Bit =
      parsedKcql.convertToBit(NSDbSinkWriter.parse(decimalRecord, Some("globalDb"), Some("globalNs"), None))

    val timestamp = bit.timestamp

    timestamp shouldBe defined

    timestamp.foreach { now =>
      val expectedBit =
        Db("globalDb")
          .namespace("globalNs")
          .metric("metric")
          .timestamp(now)
          .value(new java.math.BigDecimal("30.44"))
          .dimension("d2", new java.math.BigDecimal("15.01"))

      bit shouldBe expectedBit
    }
  }

  "SinkRecordConversion" should "successfully convert records given a kcql with value and a default value (to be ignored)" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_id AS db, string_field AS namespace, int_field AS value, d2, d1 FROM topic WITHTIMESTAMP long_field"

    val parsedKcql = ParsedKcql(withDimensionAlias, None, None, Some(new java.math.BigDecimal("1")))

    val bit: Bit =
      parsedKcql.convertToBit(NSDbSinkWriter.parse(simpleRecordWithDimentions, None, None, None))

    val expectedBit =
      Db("my_id_val")
        .namespace("foo")
        .metric("metric")
        .timestamp(12)
        .value(12)
        .dimension("d2", 12)
        .dimension("d1", "d1")

    bit shouldBe expectedBit
  }

  private val maxRetries    = 3
  private val retryInterval = 1.second
  private val timeout       = 5.seconds

  "SinkRecordConversion" should "thrown an Exception whether one of the result has 'completedSuccessfully' set to false" in {
    import scala.concurrent.ExecutionContext.Implicits.global

    def futureResult = {
      loggerImpl.info("Evaluating the future...")
      Future(
        List(RPCInsertResult(completedSuccessfully = true), RPCInsertResult(completedSuccessfully = false))
      )
    }

    an[RuntimeException] shouldBe thrownBy(
      NSDbSinkWriter.writeWithDeliveryPolicy(Some(AtLeastOnce), futureResult, maxRetries, retryInterval, timeout))
  }

  "SinkRecordConversion" should "correctly return list of successfully result" in {
    import scala.concurrent.ExecutionContext.Implicits.global

    val result       = List(RPCInsertResult(completedSuccessfully = true), RPCInsertResult(completedSuccessfully = true))
    def futureResult = Future(result)

    NSDbSinkWriter.writeWithDeliveryPolicy(Some(AtLeastOnce), futureResult, maxRetries, retryInterval, timeout) shouldBe result
  }

  "SinkRecordConversion" should "thrown an TimeoutException whether future does not complete in time" in {
    import scala.concurrent.ExecutionContext.Implicits.global

    val result: List[RPCInsertResult] =
      List(RPCInsertResult(completedSuccessfully = true), RPCInsertResult(completedSuccessfully = true))
    def futureResult = Future {
      loggerImpl.info("Evaluating the future...")
      Thread.sleep(10000)
      result
    }

    an[java.util.concurrent.TimeoutException] shouldBe thrownBy(
      NSDbSinkWriter.writeWithDeliveryPolicy(Some(AtLeastOnce), futureResult, maxRetries, retryInterval, timeout))
  }

  "SinkRecordConversion" should "thrown an Exception whether the future fails" in {

    def futureResult = {
      loggerImpl.info("Evaluating the future...")
      Future.failed[List[RPCInsertResult]](new RuntimeException("Any Reason Exception"))
    }

    an[RuntimeException] shouldBe thrownBy(
      NSDbSinkWriter.writeWithDeliveryPolicy(Some(AtLeastOnce), futureResult, maxRetries, retryInterval, timeout))
  }

  "SinkRecordConversion" should "validate semantic delivery according to the possible values" in {
    Try {
      NSDbSinkWriter.validateSemanticDelivery("testField", "AT_LEAST_ONCE")
    } shouldBe Success(Some(AtLeastOnce))

    Try {
      NSDbSinkWriter.validateSemanticDelivery("testField", "invalid-field")
    }.isFailure shouldBe true
  }

  "SinkRecordConversion" should "not convert records given a kcql without a value alias and an invalid default value" in {
    Try {
      NSDbSinkWriter.validateDefaultValue(Some("1"))
    } shouldBe Success(Some(new java.math.BigDecimal("1")))

    Try {
      NSDbSinkWriter.validateDefaultValue(Some("1.05"))
    } shouldBe Success(Some(new java.math.BigDecimal("1.05")))

    Try {
      NSDbSinkWriter.validateDefaultValue(Some("1v"))
    }.isFailure shouldBe true
  }

  "SinkRecordConversion" should "validate a duration properly" in {
    Try {
      NSDbSinkWriter.validateDuration("testField", None)
    } shouldBe Success(None)

    Try {
      NSDbSinkWriter.validateDuration("testField", Some("whatever"))
    }.isFailure shouldBe true

    Try {
      NSDbSinkWriter.validateDuration("testField", Some("2d"))
    } shouldBe Success(Some(Duration("2d")))

    Try {
      NSDbSinkWriter.validateDuration("testField", Some("2 d"))
    } shouldBe Success(Some(Duration("2 d")))

    Try {
      NSDbSinkWriter.validateDuration("testField", Some("2 days"))
    } shouldBe Success(Some(Duration("2 days")))

    Try {
      NSDbSinkWriter.validateDuration("testField", Some("2days"))
    } shouldBe Success(Some(Duration("2days")))

    Try {
      NSDbSinkWriter.validateDuration("testField", Some("1500 milliseconds"))
    } shouldBe Success(Some(Duration("1500 milliseconds")))
  }

  "SinkRecordConversion" should "successfully convert records given a kcql without a value alias and a default value" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_id AS db, string_field AS namespace, d2, d1 FROM topic WITHTIMESTAMP long_field"

    val parsedKcql = ParsedKcql(withDimensionAlias, None, None, Some(new java.math.BigDecimal("1")))

    val bit: Bit =
      parsedKcql.convertToBit(NSDbSinkWriter.parse(simpleRecordWithDimentions, None, None, None))

    val expectedBit =
      Db("my_id_val")
        .namespace("foo")
        .metric("metric")
        .timestamp(12)
        .value(1)
        .dimension("d2", 12)
        .dimension("d1", "d1")

    bit shouldBe expectedBit
  }

  "SinkRecordConversion" should "successfully convert records given a mapping without a value alias and a default value" in {
    val withDimensionAlias = Mappings(
      topic = "topic",
      metricFieldName = "metric",
      valueFieldName = None,
      timestampFieldName = Some("long_field"),
      tagsFieldName = List.empty[String]
    )

    val enrichedMapping =
      EnrichedMapping(withDimensionAlias, Some("string_id"), Some("string_field"), Some(new java.math.BigDecimal("1")))

    val bit: Bit =
      enrichedMapping.convertToBit(NSDbSinkWriter.parse(simpleRecordWithDimentions, None, None, None))

    val expectedBit =
      Db("my_id_val")
        .namespace("foo")
        .metric("metric")
        .timestamp(12)
        .value(1)
        .dimension("d2", 12)
        .dimension("int_field", 12)
        .dimension("d1", "d1")

    bit shouldBe expectedBit
  }

  "SinkRecordConversion" should "not convert records given a kcql with a nullable empty value alias and a default value" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT non_opt_field AS db, filled_opt_field AS d1, empty_opt_field AS value FROM topic WITHTIMESTAMP now"

    val result = Try {
      val parsedKcql = ParsedKcql(withDimensionAlias, None, Some("globalNs"), Some(new java.math.BigDecimal("1")))

      parsedKcql.convertToBit(NSDbSinkWriter.parse(optRecord, None, Some("globalNs"), None))
    }

    result.isFailure shouldBe true
  }

  "SinkRecordConversion" should "convert a SinkRecord given a kcql with default timestamp value now" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_id AS db, string_field AS namespace, d2 AS value, d1 FROM topic WITHTIMESTAMP now"

    val parsedKcql = ParsedKcql(withDimensionAlias, None, None, None)

    val bit: Bit =
      parsedKcql.convertToBit(NSDbSinkWriter.parse(simpleRecordWithDimentions, None, None, None))

    val timestamp = bit.timestamp

    timestamp shouldBe defined

    timestamp.foreach { now =>
      val expected = Db("my_id_val").namespace("foo").metric("metric").timestamp(now).value(12).dimension("d1", "d1")
      bit shouldBe expected
    }
  }

  "SinkRecordConversion" should "convert a SinkRecord given a kcql with default timestamp value now()" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_id AS db, string_field AS namespace, d2 AS value, d1 FROM topic WITHTIMESTAMP now()"

    val parsedKcql = ParsedKcql(withDimensionAlias, None, None, None)

    val bit: Bit =
      parsedKcql.convertToBit(NSDbSinkWriter.parse(simpleRecordWithDimentions, None, None, None))

    val timestamp = bit.timestamp

    timestamp shouldBe defined

    timestamp.foreach { now =>
      val expected = Db("my_id_val").namespace("foo").metric("metric").timestamp(now).value(12).dimension("d1", "d1")
      bit shouldBe expected
    }
  }

  "SinkRecordConversion" should "convert a SinkRecord given a kcql with default timestamp value sys_time" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_id AS db, string_field AS namespace, d2 AS value, d1 FROM topic WITHTIMESTAMP sys_time"

    val parsedKcql = ParsedKcql(withDimensionAlias, None, None, None)

    val bit: Bit =
      parsedKcql.convertToBit(NSDbSinkWriter.parse(simpleRecordWithDimentions, None, None, None))

    val timestamp = bit.timestamp

    timestamp shouldBe defined

    timestamp.map { now =>
      val expected = Db("my_id_val").namespace("foo").metric("metric").timestamp(now).value(12).dimension("d1", "d1")
      bit shouldBe expected
    }
  }

  "SinkRecordConversion" should "convert a SinkRecord given a kcql with default timestamp value sys_time()" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_id AS db, string_field AS namespace, d2 AS value, d1 FROM topic WITHTIMESTAMP sys_time()"

    val parsedKcql = ParsedKcql(withDimensionAlias, None, None, None)

    val bit: Bit =
      parsedKcql.convertToBit(NSDbSinkWriter.parse(simpleRecordWithDimentions, None, None, None))

    val timestamp = bit.timestamp

    timestamp shouldBe defined

    timestamp.map { now =>
      val expected = Db("my_id_val").namespace("foo").metric("metric").timestamp(now).value(12).dimension("d1", "d1")
      bit shouldBe expected
    }
  }

  "SinkRecordConversion" should "convert a SinkRecord given a kcql with default timestamp value current_time" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_id AS db, string_field AS namespace, d2 AS value, d1 FROM topic WITHTIMESTAMP current_time"

    val parsedKcql = ParsedKcql(withDimensionAlias, None, None, None)

    val bit: Bit =
      parsedKcql.convertToBit(NSDbSinkWriter.parse(simpleRecordWithDimentions, None, None, None))

    val timestamp = bit.timestamp

    timestamp shouldBe defined

    timestamp.map { now =>
      val expected = Db("my_id_val").namespace("foo").metric("metric").timestamp(now).value(12).dimension("d1", "d1")
      bit shouldBe expected
    }
  }

  "SinkRecordConversion" should "convert a SinkRecord given a kcql with default timestamp value current_time()" in {
    val withDimensionAlias =
      "INSERT INTO metric SELECT string_id AS db, string_field AS namespace, d2 AS value, d1 FROM topic WITHTIMESTAMP current_time()"

    val parsedKcql = ParsedKcql(withDimensionAlias, None, None, None)

    val bit: Bit =
      parsedKcql.convertToBit(NSDbSinkWriter.parse(simpleRecordWithDimentions, None, None, None))

    val timestamp = bit.timestamp

    timestamp shouldBe defined

    timestamp.map { now =>
      val expected = Db("my_id_val").namespace("foo").metric("metric").timestamp(now).value(12).dimension("d1", "d1")
      bit shouldBe expected
    }
  }

  "SinkRecordConversion" should "convert a SinkRecord given a transform with default timestamp value current_time()" in {

    val withDimensionAlias = Mappings(
      topic = "topic",
      metricFieldName = "metric",
      valueFieldName = Some("d2"),
      timestampFieldName = None,
      tagsFieldName = List.empty[String]
    )

    val enrichedMapping =
      EnrichedMapping(withDimensionAlias, Some("string_id"), Some("string_field"), None)

    val bit: Bit =
      enrichedMapping.convertToBit(NSDbSinkWriter.parse(simpleRecordWithDimentions, None, None, None))

    val timestamp = bit.timestamp

    timestamp shouldBe defined

    timestamp.map { now =>
      val expected = Db("my_id_val")
        .namespace("foo")
        .metric("metric")
        .timestamp(now)
        .value(12)
        .dimension("long_field", 12L)
        .dimension("int_field", 12)
        .dimension("d1", "d1")
      bit shouldBe expected
    }
  }

}
