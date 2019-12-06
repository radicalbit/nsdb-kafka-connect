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

import io.radicalbit.nsdb.api.scala.{Bit, Db}

object QueryTransform {
  def apply(transform: Transform,
            globalDb: Option[String],
            globalNamespace: Option[String],
            defaultValue: Option[java.math.BigDecimal]): IQuery = {
    require(globalDb.isDefined, "A global db configuration be defined")
    require(globalNamespace.isDefined, "A global namespace configuration must be defined")
    require(transform.metricFieldName.nonEmpty, "Metric value in Transform must be defined")
    require(
      transform.valueFieldName.nonEmpty || defaultValue.isDefined,
      "Value alias in transform must be defined"
    )

    QueryTransform(
      globalDb.get,
      globalNamespace.get,
      transform.metricFieldName,
      defaultValue,
      transform.timestampFieldName.fold(Some("now"))(Some(_)),
      transform.valueFieldName,
      transform.tagsFieldName
    )
  }
}
final case class QueryTransform(dbField: String,
                                namespaceField: String,
                                metric: String,
                                defaultValue: Option[java.math.BigDecimal],
                                timestampField: Option[String],
                                valueField: Option[String],
                                tagsNames: List[String])
    extends IQuery {

  def convertToBit(valuesMap: Map[String, Any]): Bit = {
    require(valuesMap.get(dbField).isDefined && valuesMap(dbField).isInstanceOf[String],
            s"required field $dbField is missing from record or is invalid")
    require(valuesMap.get(namespaceField).isDefined,
            s"required field $namespaceField is missing from record or is invalid")

    var bit: Bit =
      Db(valuesMap(dbField).toString).namespace(valuesMap(namespaceField).toString).metric(metric)

    timestampField.flatMap {
      case f if defaultTimestampKeywords.contains(f) => Some(System.currentTimeMillis())
      case f                                         => valuesMap.get(f)
    } match {
      case Some(t: Long) => bit = bit.timestamp(t)
      case Some(v)       => sys.error(s"Type ${v.getClass} is not supported for timestamp field")
      case None          => sys.error(s"Timestamp is not defined in record and a valid default is not provided")
    }

    valueField match {
      case Some(valueField) =>
        valuesMap.get(valueField) match {
          case Some(v: Int)                  => bit = bit.value(v)
          case Some(v: Long)                 => bit = bit.value(v)
          case Some(v: Double)               => bit = bit.value(v)
          case Some(v: Float)                => bit = bit.value(v)
          case Some(v: java.math.BigDecimal) => bit = bit.value(v)
          case Some(unsupportedValue) =>
            sys.error(s"Type ${Option(unsupportedValue).map(_.getClass)} is not supported for value field")
          case None =>
            sys.error(
              s"Value not found. Value field cannot be a nullable field and a default value is required if it has not been chosen from input.")

        }
      case None =>
        defaultValue match {
          case Some(dv) => bit = bit.value(dv)
          case None     => sys.error(s"Value is not defined in record and a default is not provided")
        }
    }

    val dimensionsNames = valuesMap -- tagsNames - timestampField.getOrElse("") - valueField.getOrElse("") - dbField - namespaceField

    dimensionsNames.foreach {
      case (key, value) =>
        value match {
          case v: Int                  => bit = bit.dimension(key, v)
          case v: Long                 => bit = bit.dimension(key, v)
          case v: Double               => bit = bit.dimension(key, v)
          case v: Float                => bit = bit.dimension(key, v)
          case v: String               => bit = bit.dimension(key, v)
          case v: java.math.BigDecimal => bit = bit.dimension(key, v)
          case Some(unsupportedValue) =>
            sys.error(s"Type ${Option(unsupportedValue).map(_.getClass)} is not supported for dimensions")
          case None => ()
        }
    }

    tagsNames.foreach {
      case name =>
        valuesMap.get(name) match {
          case Some(v: Int)                  => bit = bit.tag(name, v)
          case Some(v: Long)                 => bit = bit.tag(name, v)
          case Some(v: Double)               => bit = bit.tag(name, v)
          case Some(v: Float)                => bit = bit.tag(name, v)
          case Some(v: String)               => bit = bit.tag(name, v)
          case Some(v: java.math.BigDecimal) => bit = bit.tag(name, v)
          case Some(unsupportedValue) =>
            sys.error(s"Type ${Option(unsupportedValue).map(_.getClass)} is not supported for tags")
          case None => ()
        }
    }

    bit
  }
}
