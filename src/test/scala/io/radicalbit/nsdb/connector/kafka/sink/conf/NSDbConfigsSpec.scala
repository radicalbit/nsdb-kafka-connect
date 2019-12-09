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
import org.apache.kafka.common.config.ConfigDef
import org.apache.kafka.common.config.ConfigDef.{Importance, Type}
import org.scalatest.{FlatSpec, Matchers, OneInstancePerTest}

class NSDbConfigsSpec extends FlatSpec with Matchers with OneInstancePerTest {

  "NsdbConfigs" should "have the right configurations" in {
    val configKeys = NSDbConfigs.configDef.configKeys()

    configKeys.get(NSDbConfigs.NSDB_HOST).name shouldBe NSDbConfigs.NSDB_HOST
    configKeys.get(NSDbConfigs.NSDB_HOST).`type` shouldBe Type.STRING
    configKeys.get(NSDbConfigs.NSDB_HOST).defaultValue shouldBe NSDbConfigs.NSDB_HOST_DEFAULT
    configKeys.get(NSDbConfigs.NSDB_HOST).importance shouldBe Importance.HIGH
    configKeys.get(NSDbConfigs.NSDB_HOST).documentation shouldBe NSDbConfigs.NSDB_HOST_DOC
    configKeys.get(NSDbConfigs.NSDB_HOST).group shouldBe "Connection"
    configKeys.get(NSDbConfigs.NSDB_HOST).orderInGroup shouldBe 1
    configKeys.get(NSDbConfigs.NSDB_HOST).width shouldBe ConfigDef.Width.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_HOST).displayName shouldBe NSDbConfigs.NSDB_HOST

    configKeys.get(NSDbConfigs.NSDB_PORT).name shouldBe NSDbConfigs.NSDB_PORT
    configKeys.get(NSDbConfigs.NSDB_PORT).`type` shouldBe Type.INT
    configKeys.get(NSDbConfigs.NSDB_PORT).defaultValue shouldBe NSDbConfigs.NSDB_PORT_DEFAULT
    configKeys.get(NSDbConfigs.NSDB_PORT).importance shouldBe Importance.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_PORT).documentation shouldBe NSDbConfigs.NSDB_PORT_DOC
    configKeys.get(NSDbConfigs.NSDB_PORT).group shouldBe "Connection"
    configKeys.get(NSDbConfigs.NSDB_PORT).orderInGroup shouldBe 2
    configKeys.get(NSDbConfigs.NSDB_PORT).width shouldBe ConfigDef.Width.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_PORT).displayName shouldBe NSDbConfigs.NSDB_PORT

    configKeys.get(NSDbConfigs.NSDB_KCQL).name shouldBe NSDbConfigs.NSDB_KCQL
    configKeys.get(NSDbConfigs.NSDB_KCQL).`type` shouldBe Type.STRING
    configKeys.get(NSDbConfigs.NSDB_KCQL).importance shouldBe Importance.HIGH
    configKeys.get(NSDbConfigs.NSDB_KCQL).documentation shouldBe NSDbConfigs.NSDB_KCQL_DOC
    configKeys.get(NSDbConfigs.NSDB_KCQL).group shouldBe "Connection"
    configKeys.get(NSDbConfigs.NSDB_KCQL).orderInGroup shouldBe 3
    configKeys.get(NSDbConfigs.NSDB_KCQL).width shouldBe ConfigDef.Width.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_KCQL).displayName shouldBe NSDbConfigs.NSDB_KCQL

    configKeys.get(NSDbConfigs.NSDB_DB).name shouldBe NSDbConfigs.NSDB_DB
    configKeys.get(NSDbConfigs.NSDB_DB).`type` shouldBe Type.STRING
    configKeys.get(NSDbConfigs.NSDB_DB).defaultValue shouldBe null
    configKeys.get(NSDbConfigs.NSDB_DB).importance shouldBe Importance.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_DB).documentation shouldBe NSDbConfigs.NSDB_DB_DOC

    configKeys.get(NSDbConfigs.NSDB_NAMESPACE).name shouldBe NSDbConfigs.NSDB_NAMESPACE
    configKeys.get(NSDbConfigs.NSDB_NAMESPACE).`type` shouldBe Type.STRING
    configKeys.get(NSDbConfigs.NSDB_NAMESPACE).defaultValue shouldBe null
    configKeys.get(NSDbConfigs.NSDB_NAMESPACE).importance shouldBe Importance.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_NAMESPACE).documentation shouldBe NSDbConfigs.NSDB_NAMESPACE_DOC

    configKeys.get(NSDbConfigs.NSDB_DEFAULT_VALUE).name shouldBe NSDbConfigs.NSDB_DEFAULT_VALUE
    configKeys.get(NSDbConfigs.NSDB_DEFAULT_VALUE).`type` shouldBe Type.STRING
    configKeys.get(NSDbConfigs.NSDB_DEFAULT_VALUE).defaultValue shouldBe null
    configKeys.get(NSDbConfigs.NSDB_DEFAULT_VALUE).importance shouldBe Importance.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_DEFAULT_VALUE).documentation shouldBe NSDbConfigs.NSDB_DEFAULT_VALUE_DOC

    configKeys.get(NSDbConfigs.NSDB_METRIC_RETENTION_POLICY).name shouldBe NSDbConfigs.NSDB_METRIC_RETENTION_POLICY
    configKeys.get(NSDbConfigs.NSDB_METRIC_RETENTION_POLICY).`type` shouldBe Type.STRING
    configKeys.get(NSDbConfigs.NSDB_METRIC_RETENTION_POLICY).defaultValue shouldBe null
    configKeys.get(NSDbConfigs.NSDB_METRIC_RETENTION_POLICY).importance shouldBe Importance.MEDIUM
    configKeys
      .get(NSDbConfigs.NSDB_METRIC_RETENTION_POLICY)
      .documentation shouldBe NSDbConfigs.NSDB_METRIC_RETENTION_POLICY_DOC
    configKeys.get(NSDbConfigs.NSDB_METRIC_RETENTION_POLICY).group shouldBe "Metric Init Params"
    configKeys.get(NSDbConfigs.NSDB_METRIC_RETENTION_POLICY).orderInGroup shouldBe 1
    configKeys.get(NSDbConfigs.NSDB_METRIC_RETENTION_POLICY).width shouldBe ConfigDef.Width.MEDIUM
    configKeys
      .get(NSDbConfigs.NSDB_METRIC_RETENTION_POLICY)
      .displayName shouldBe NSDbConfigs.NSDB_METRIC_RETENTION_POLICY

    configKeys.get(NSDbConfigs.NSDB_SHARD_INTERVAL).name shouldBe NSDbConfigs.NSDB_SHARD_INTERVAL
    configKeys.get(NSDbConfigs.NSDB_SHARD_INTERVAL).`type` shouldBe Type.STRING
    configKeys.get(NSDbConfigs.NSDB_SHARD_INTERVAL).defaultValue shouldBe null
    configKeys.get(NSDbConfigs.NSDB_SHARD_INTERVAL).importance shouldBe Importance.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_SHARD_INTERVAL).documentation shouldBe NSDbConfigs.NSDB_SHARD_INTERVAL_DOC
    configKeys.get(NSDbConfigs.NSDB_SHARD_INTERVAL).group shouldBe "Metric Init Params"
    configKeys.get(NSDbConfigs.NSDB_SHARD_INTERVAL).orderInGroup shouldBe 2
    configKeys.get(NSDbConfigs.NSDB_SHARD_INTERVAL).width shouldBe ConfigDef.Width.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_SHARD_INTERVAL).displayName shouldBe NSDbConfigs.NSDB_SHARD_INTERVAL

    configKeys.get(NSDbConfigs.NSDB_TIMEOUT).name shouldBe NSDbConfigs.NSDB_TIMEOUT
    configKeys.get(NSDbConfigs.NSDB_TIMEOUT).`type` shouldBe Type.STRING
    configKeys.get(NSDbConfigs.NSDB_TIMEOUT).defaultValue shouldBe NSDbConfigs.NSDB_TIMEOUT_DEFAULT
    configKeys.get(NSDbConfigs.NSDB_TIMEOUT).importance shouldBe Importance.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_TIMEOUT).documentation shouldBe NSDbConfigs.NSDB_TIMEOUT_DOC
    configKeys.get(NSDbConfigs.NSDB_TIMEOUT).group shouldBe "Connection"
    configKeys.get(NSDbConfigs.NSDB_TIMEOUT).orderInGroup shouldBe 4
    configKeys.get(NSDbConfigs.NSDB_TIMEOUT).width shouldBe ConfigDef.Width.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_TIMEOUT).displayName shouldBe NSDbConfigs.NSDB_TIMEOUT

    configKeys.get(NSDbConfigs.NSDB_SEMANTIC_DELIVERY).name shouldBe NSDbConfigs.NSDB_SEMANTIC_DELIVERY
    configKeys.get(NSDbConfigs.NSDB_SEMANTIC_DELIVERY).`type` shouldBe Type.STRING
    configKeys.get(NSDbConfigs.NSDB_SEMANTIC_DELIVERY).defaultValue shouldBe NSDbConfigs.NSDB_SEMANTIC_DELIVERY_DEFAULT
    configKeys.get(NSDbConfigs.NSDB_SEMANTIC_DELIVERY).importance shouldBe Importance.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_SEMANTIC_DELIVERY).documentation shouldBe NSDbConfigs.NSDB_SEMANTIC_DELIVERY_DOC
    configKeys.get(NSDbConfigs.NSDB_SEMANTIC_DELIVERY).group shouldBe "Semantic Delivery"
    configKeys.get(NSDbConfigs.NSDB_SEMANTIC_DELIVERY).orderInGroup shouldBe 1
    configKeys.get(NSDbConfigs.NSDB_SEMANTIC_DELIVERY).width shouldBe ConfigDef.Width.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_SEMANTIC_DELIVERY).displayName shouldBe NSDbConfigs.NSDB_SEMANTIC_DELIVERY

    configKeys.get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRIES).name shouldBe NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRIES
    configKeys.get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRIES).`type` shouldBe Type.INT
    configKeys
      .get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRIES)
      .defaultValue shouldBe NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRIES_DEFAULT
    configKeys.get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRIES).importance shouldBe Importance.LOW
    configKeys
      .get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRIES)
      .documentation shouldBe NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRIES_DOC
    configKeys.get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRIES).group shouldBe "Semantic Delivery"
    configKeys.get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRIES).orderInGroup shouldBe 2
    configKeys.get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRIES).width shouldBe ConfigDef.Width.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRIES).displayName shouldBe NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRIES

    configKeys
      .get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL)
      .name shouldBe NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL
    configKeys.get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL).`type` shouldBe Type.STRING
    configKeys
      .get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL)
      .defaultValue shouldBe NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL_DEFAULT
    configKeys.get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL).importance shouldBe Importance.LOW
    configKeys
      .get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL)
      .documentation shouldBe NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL_DOC
    configKeys.get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL).group shouldBe "Semantic Delivery"
    configKeys.get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL).orderInGroup shouldBe 3
    configKeys.get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL).width shouldBe ConfigDef.Width.MEDIUM
    configKeys
      .get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL)
      .displayName shouldBe NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL

    configKeys.get(NSDbConfigs.NSDB_MAPPING_METRICS).name shouldBe NSDbConfigs.NSDB_MAPPING_METRICS
    configKeys.get(NSDbConfigs.NSDB_MAPPING_METRICS).`type` shouldBe Type.STRING
    configKeys.get(NSDbConfigs.NSDB_MAPPING_METRICS).defaultValue shouldBe null
    configKeys.get(NSDbConfigs.NSDB_MAPPING_METRICS).importance shouldBe Importance.HIGH
    configKeys.get(NSDbConfigs.NSDB_MAPPING_METRICS).documentation shouldBe NSDbConfigs.NSDB_MAPPING_METRICS_DOC
    configKeys.get(NSDbConfigs.NSDB_MAPPING_METRICS).group shouldBe "Mapping Configuration"
    configKeys.get(NSDbConfigs.NSDB_MAPPING_METRICS).orderInGroup shouldBe 1
    configKeys.get(NSDbConfigs.NSDB_MAPPING_METRICS).width shouldBe ConfigDef.Width.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_MAPPING_METRICS).displayName shouldBe NSDbConfigs.NSDB_MAPPING_METRICS

    configKeys.get(NSDbConfigs.NSDB_MAPPING_VALUES).name shouldBe NSDbConfigs.NSDB_MAPPING_VALUES
    configKeys.get(NSDbConfigs.NSDB_MAPPING_VALUES).`type` shouldBe Type.STRING
    configKeys.get(NSDbConfigs.NSDB_MAPPING_VALUES).defaultValue shouldBe null
    configKeys.get(NSDbConfigs.NSDB_MAPPING_VALUES).importance shouldBe Importance.HIGH
    configKeys.get(NSDbConfigs.NSDB_MAPPING_VALUES).documentation shouldBe NSDbConfigs.NSDB_MAPPING_VALUES_DOC
    configKeys.get(NSDbConfigs.NSDB_MAPPING_VALUES).group shouldBe "Mapping Configuration"
    configKeys.get(NSDbConfigs.NSDB_MAPPING_VALUES).orderInGroup shouldBe 2
    configKeys.get(NSDbConfigs.NSDB_MAPPING_VALUES).width shouldBe ConfigDef.Width.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_MAPPING_VALUES).displayName shouldBe NSDbConfigs.NSDB_MAPPING_VALUES

    configKeys.get(NSDbConfigs.NSDB_MAPPING_TAGS).name shouldBe NSDbConfigs.NSDB_MAPPING_TAGS
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TAGS).`type` shouldBe Type.STRING
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TAGS).defaultValue shouldBe null
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TAGS).importance shouldBe Importance.HIGH
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TAGS).documentation shouldBe NSDbConfigs.NSDB_MAPPING_TAGS_DOC
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TAGS).group shouldBe "Mapping Configuration"
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TAGS).orderInGroup shouldBe 3
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TAGS).width shouldBe ConfigDef.Width.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TAGS).displayName shouldBe NSDbConfigs.NSDB_MAPPING_TAGS

    configKeys.get(NSDbConfigs.NSDB_MAPPING_TIMESTAMPS).name shouldBe NSDbConfigs.NSDB_MAPPING_TIMESTAMPS
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TIMESTAMPS).`type` shouldBe Type.STRING
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TIMESTAMPS).defaultValue shouldBe null
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TIMESTAMPS).importance shouldBe Importance.HIGH
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TIMESTAMPS).documentation shouldBe NSDbConfigs.NSDB_MAPPING_TIMESTAMPS_DOC
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TIMESTAMPS).group shouldBe "Mapping Configuration"
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TIMESTAMPS).orderInGroup shouldBe 4
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TIMESTAMPS).width shouldBe ConfigDef.Width.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_MAPPING_TIMESTAMPS).displayName shouldBe NSDbConfigs.NSDB_MAPPING_TIMESTAMPS

  }
}
