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
