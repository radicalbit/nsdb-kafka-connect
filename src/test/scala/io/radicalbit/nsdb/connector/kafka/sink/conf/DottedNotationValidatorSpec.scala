package io.radicalbit.nsdb.connector.kafka.sink.conf

import org.apache.kafka.common.config.ConfigException
import org.scalatest.{FlatSpec, Matchers}

class DottedNotationValidatorSpec extends FlatSpec with Matchers {
  val fieldName = "test-field"

  "DottedNotationValidator" should "correctly validate null value" in {
    DottedNotationValidator.ensureValid(fieldName, null)
  }
  "DottedNotationValidator" should "throw ConfigException for empty string value" in {
    an[ConfigException] shouldBe thrownBy(DottedNotationValidator.ensureValid(fieldName, ""))
  }
  "DottedNotationValidator" should "throw ConfigException for invalid format value" in {
    an[ConfigException] shouldBe thrownBy(DottedNotationValidator.ensureValid(fieldName, "invalid-format"))
  }
  "DottedNotationValidator" should "correctly validate valid.format value" in {
    DottedNotationValidator.ensureValid(fieldName, "valid.format")
  }
  "DottedNotationValidator" should "throw ConfigException for invalid format value 2" in {
    an[ConfigException] shouldBe thrownBy(DottedNotationValidator.ensureValid(fieldName, "valid.field,invalid-field"))
  }

}
