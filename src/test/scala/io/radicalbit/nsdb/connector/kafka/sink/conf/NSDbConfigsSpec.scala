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

    configKeys.get(NSDbConfigs.NSDB_SEMANTIC_DELIVERY).name shouldBe NSDbConfigs.NSDB_SEMANTIC_DELIVERY
    configKeys.get(NSDbConfigs.NSDB_SEMANTIC_DELIVERY).`type` shouldBe Type.STRING
    configKeys.get(NSDbConfigs.NSDB_SEMANTIC_DELIVERY).defaultValue shouldBe NSDbConfigs.NSDB_SEMANTIC_DELIVERY_DEFAULT
    configKeys.get(NSDbConfigs.NSDB_SEMANTIC_DELIVERY).importance shouldBe Importance.MEDIUM
    configKeys.get(NSDbConfigs.NSDB_SEMANTIC_DELIVERY).documentation shouldBe NSDbConfigs.NSDB_SEMANTIC_DELIVERY_DOC

  }
}
