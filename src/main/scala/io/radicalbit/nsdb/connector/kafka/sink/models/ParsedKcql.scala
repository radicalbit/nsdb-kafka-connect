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

import com.datamountaineer.kcql.Kcql
import io.radicalbit.nsdb.api.scala.{Bit, Db}
import io.radicalbit.nsdb.connector.kafka.sink.conf.Constants._

import scala.collection.JavaConverters._

/**
  * Parsed information from a kcql expression.
  * For all the aliases maps `alias -> field` means that `field` must be fetched from topic and saved as `alias` to NSDb.
  * @param dbField NSDb db field to be fetched from topic data.
  * @param namespaceField NSDb namespace field to be fetched from topic data.
  * @param metric NSDb metric.
  * @param timestampField timestamp field if present (current timestamp is used otherwise).
  * @param valueField value field if present (default value is used otherwise).
  * @param tagAliasesMap NSDb tags aliases map.
  * @param dimensionAliasesMap NSDb dimensions aliases map.
  */
final case class ParsedKcql(dbField: String,
                            namespaceField: String,
                            metric: String,
                            defaultValue: Option[java.math.BigDecimal],
                            timestampField: Option[String],
                            valueField: Option[String],
                            tagAliasesMap: Map[String, String],
                            dimensionAliasesMap: Map[String, String])
    extends IQuery {

  override def convertToBit(valuesMap: Map[String, Any]): Bit = {

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

    dimensionAliasesMap.foreach {
      case (alias, name) =>
        valuesMap.get(name) match {
          case Some(v: Int)                  => bit = bit.dimension(alias, v)
          case Some(v: Long)                 => bit = bit.dimension(alias, v)
          case Some(v: Double)               => bit = bit.dimension(alias, v)
          case Some(v: Float)                => bit = bit.dimension(alias, v)
          case Some(v: String)               => bit = bit.dimension(alias, v)
          case Some(v: java.math.BigDecimal) => bit = bit.dimension(alias, v)
          case Some(unsupportedValue) =>
            sys.error(s"Type ${Option(unsupportedValue).map(_.getClass)} is not supported for dimensions")
          case None => ()
        }
    }

    tagAliasesMap.foreach {
      case (alias, name) =>
        valuesMap.get(name) match {
          case Some(v: Int)                  => bit = bit.tag(alias, v)
          case Some(v: Long)                 => bit = bit.tag(alias, v)
          case Some(v: Double)               => bit = bit.tag(alias, v)
          case Some(v: Float)                => bit = bit.tag(alias, v)
          case Some(v: String)               => bit = bit.tag(alias, v)
          case Some(v: java.math.BigDecimal) => bit = bit.tag(alias, v)
          case Some(unsupportedValue) =>
            sys.error(s"Type ${Option(unsupportedValue).map(_.getClass)} is not supported for tags")
          case None => ()
        }
    }

    bit
  }
}

object ParsedKcql {

  /**
    * Returns an instance of [[ParsedKcql]] from a kcql string.
    * @param queryString the string to be parsed.
    * @param globalDb the db defined as a config param if present.
    * @param globalNamespace the namespace defined as a config param if present.
    * @param defaultValue the default value defined as a config param if present.
    * @return the instance of [[ParsedKcql]].
    * @throws IllegalArgumentException if queryString is not valid.
    */
  def apply(queryString: String,
            globalDb: Option[String],
            globalNamespace: Option[String],
            defaultValue: Option[java.math.BigDecimal]): IQuery = {
    this(Kcql.parse(queryString), globalDb, globalNamespace, defaultValue)
  }

  /**
    * Returns an instance of [[ParsedKcql]] from a [[Kcql]].
    * @param kcql the kcql to be parsed.
    * @param globalDb the db defined as a config param if present. In values map, will be used both for key and value.
    * @param globalNamespace the namespace defined as a config param if present.
    *                        In values map, will be used both for key and value.
    * @param defaultValue the default value defined as a config param if present.
    * @return the instance of [[ParsedKcql]].
    * @throws IllegalArgumentException if input kcql is not valid.
    */
  def apply(kcql: Kcql,
            globalDb: Option[String],
            globalNamespace: Option[String],
            defaultValue: Option[java.math.BigDecimal]): IQuery = {

    val aliasesMap = kcql.getFields.asScala.map(f => f.getAlias -> f.getName).toMap

    val db        = aliasesMap.get(Parsed.DbFieldName) orElse globalDb
    val namespace = aliasesMap.get(Parsed.NamespaceFieldName) orElse globalNamespace
    val metric    = kcql.getTarget

    val tagAliases = Option(kcql.getTags)
      .map(_.asScala)
      .getOrElse(List.empty)
      .map(e => e.getKey -> Option(e.getValue).getOrElse(e.getKey))
      .toMap

    require(db.isDefined, "A global db configuration or a Db alias in Kcql must be defined")
    require(namespace.isDefined, "A global namespace configuration or a Namespace alias in Kcql must be defined")
    require(
      aliasesMap.get(Writer.ValueFieldName).isDefined || defaultValue.isDefined,
      "Value alias in kcql must be defined"
    )

    val allAliases = aliasesMap - Parsed.DbFieldName - Parsed.NamespaceFieldName - Writer.ValueFieldName

    val (tags: Map[String, String], dimensions: Map[String, String]) = allAliases.partition {
      case (k, v) => tagAliases.get(k).isDefined
    }

    ParsedKcql(
      db.get,
      namespace.get,
      metric,
      defaultValue,
      Option(kcql.getTimestamp),
      aliasesMap.get(Writer.ValueFieldName),
      tags,
      dimensions
    )
  }
}
