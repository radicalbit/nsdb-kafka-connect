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

import org.apache.kafka.common.config.{ConfigDef, ConfigException}

/**
  * Kafka Connect Dotted Notation Validator (valid format: `topicName.fieldName`)
  */
object DottedNotationValidator extends ConfigDef.Validator {
  // format: [topicName1.fieldName,topicName2.fieldName]
  private val dottedNotation = "[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+(,[a-zA-Z0-9_]+\\.[a-zA-Z0-9_]+)*".r

  def ensureValid(name: String, value: Any): Unit = {
    Option(value).foreach { v =>
      if (!v.toString.matches(dottedNotation.regex))
        throw new ConfigException(name, value, "Bad field format. Expected: [topicName.fieldName,topicName.fieldName]")
    }
  }
}
