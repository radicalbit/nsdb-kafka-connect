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

package io.radicalbit.nsdb.connector.kafka.sink.models

import io.radicalbit.nsdb.api.scala.Bit

abstract class IQuery {
  def dbField: String
  def namespaceField: String
  def metric: String
  def defaultValue: Option[java.math.BigDecimal]
  def timestampField: Option[String]
  def valueField: Option[String]
//  def tagAliasesMap: Map[String, String]
  //  def dimensionAliasesMap: Map[String, String]

  def convertToBit(valuesMap: Map[String, Any]): Bit

  protected val defaultTimestampKeywords =
    Set("now", "now()", "sys_time", "sys_time()", "current_time", "current_time()")
}
