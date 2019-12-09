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

/**
  * Singleton aimed to collect constant values along the connector.
  */
object Constants {

  object Writer {
    final lazy val TimestampFieldName = "timestamp"
    final lazy val ValueFieldName     = "value"
  }

  object Parsed {
    final lazy val DbFieldName        = "db"
    final lazy val NamespaceFieldName = "namespace"
  }

  sealed trait SemanticDelivery {
    def value: String
  }
  case object AtMostOnce extends SemanticDelivery {
    val value: String = "at_most_once"
  }
  case object AtLeastOnce extends SemanticDelivery {
    val value: String = "at_least_once"
  }

  object SemanticDelivery {
    final lazy val possibleValues = List(AtMostOnce.value, AtLeastOnce.value)

    def parse(s: String): Option[SemanticDelivery] =
      s.toLowerCase match {
        case AtMostOnce.value  => Some(AtMostOnce)
        case AtLeastOnce.value => Some(AtLeastOnce)
        case _                 => None
      }
  }

  object Type {
    def parse(s: String): Option[Type] =
      s.toLowerCase match {
        case KcqlType.value    => Some(KcqlType)
        case MappingType.value => Some(MappingType)
        case _                 => None
      }
  }

  sealed trait Type {
    def value: String
  }
  case object KcqlType extends Type {
    val value: String = "kcql-type"
  }
  case object MappingType extends Type {
    val value: String = "mapping-type"
  }

}
