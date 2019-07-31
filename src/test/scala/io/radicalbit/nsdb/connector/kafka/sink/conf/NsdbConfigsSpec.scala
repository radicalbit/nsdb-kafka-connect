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

class NsdbConfigsSpec extends FlatSpec with Matchers with OneInstancePerTest {

  "NsdbConfigs" should "have the right configurations" in {
    val configKeys = NsdbConfigs.configDef.configKeys()

    configKeys.get(NsdbConfigs.NSDB_HOST).name shouldBe NsdbConfigs.NSDB_HOST
    configKeys.get(NsdbConfigs.NSDB_HOST).`type` shouldBe Type.STRING
    configKeys.get(NsdbConfigs.NSDB_HOST).defaultValue shouldBe NsdbConfigs.NSDB_HOST_DEFAULT
    configKeys.get(NsdbConfigs.NSDB_HOST).importance shouldBe Importance.HIGH
    configKeys.get(NsdbConfigs.NSDB_HOST).documentation shouldBe NsdbConfigs.NSDB_HOST_DOC
    configKeys.get(NsdbConfigs.NSDB_HOST).group shouldBe "Connection"
    configKeys.get(NsdbConfigs.NSDB_HOST).orderInGroup shouldBe 1
    configKeys.get(NsdbConfigs.NSDB_HOST).width shouldBe ConfigDef.Width.MEDIUM
    configKeys.get(NsdbConfigs.NSDB_HOST).displayName shouldBe NsdbConfigs.NSDB_HOST

    configKeys.get(NsdbConfigs.NSDB_PORT).name shouldBe NsdbConfigs.NSDB_PORT
    configKeys.get(NsdbConfigs.NSDB_PORT).`type` shouldBe Type.INT
    configKeys.get(NsdbConfigs.NSDB_PORT).defaultValue shouldBe NsdbConfigs.NSDB_PORT_DEFAULT
    configKeys.get(NsdbConfigs.NSDB_PORT).importance shouldBe Importance.MEDIUM
    configKeys.get(NsdbConfigs.NSDB_PORT).documentation shouldBe NsdbConfigs.NSDB_PORT_DOC
    configKeys.get(NsdbConfigs.NSDB_PORT).group shouldBe "Connection"
    configKeys.get(NsdbConfigs.NSDB_PORT).orderInGroup shouldBe 2
    configKeys.get(NsdbConfigs.NSDB_PORT).width shouldBe ConfigDef.Width.MEDIUM
    configKeys.get(NsdbConfigs.NSDB_PORT).displayName shouldBe NsdbConfigs.NSDB_PORT

    configKeys.get(NsdbConfigs.NSDB_KCQL).name shouldBe NsdbConfigs.NSDB_KCQL
    configKeys.get(NsdbConfigs.NSDB_KCQL).`type` shouldBe Type.STRING
    configKeys.get(NsdbConfigs.NSDB_KCQL).importance shouldBe Importance.HIGH
    configKeys.get(NsdbConfigs.NSDB_KCQL).documentation shouldBe NsdbConfigs.NSDB_KCQL_DOC
    configKeys.get(NsdbConfigs.NSDB_KCQL).group shouldBe "Connection"
    configKeys.get(NsdbConfigs.NSDB_KCQL).orderInGroup shouldBe 3
    configKeys.get(NsdbConfigs.NSDB_KCQL).width shouldBe ConfigDef.Width.MEDIUM
    configKeys.get(NsdbConfigs.NSDB_KCQL).displayName shouldBe NsdbConfigs.NSDB_KCQL

    configKeys.get(NsdbConfigs.NSDB_DB).name shouldBe NsdbConfigs.NSDB_DB
    configKeys.get(NsdbConfigs.NSDB_DB).`type` shouldBe Type.STRING
    configKeys.get(NsdbConfigs.NSDB_DB).defaultValue shouldBe null
    configKeys.get(NsdbConfigs.NSDB_DB).importance shouldBe Importance.MEDIUM
    configKeys.get(NsdbConfigs.NSDB_DB).documentation shouldBe NsdbConfigs.NSDB_DB_DOC

    configKeys.get(NsdbConfigs.NSDB_NAMESPACE).name shouldBe NsdbConfigs.NSDB_NAMESPACE
    configKeys.get(NsdbConfigs.NSDB_NAMESPACE).`type` shouldBe Type.STRING
    configKeys.get(NsdbConfigs.NSDB_NAMESPACE).defaultValue shouldBe null
    configKeys.get(NsdbConfigs.NSDB_NAMESPACE).importance shouldBe Importance.MEDIUM
    configKeys.get(NsdbConfigs.NSDB_NAMESPACE).documentation shouldBe NsdbConfigs.NSDB_NAMESPACE_DOC

    configKeys.get(NsdbConfigs.NSDB_DEFAULT_VALUE).name shouldBe NsdbConfigs.NSDB_DEFAULT_VALUE
    configKeys.get(NsdbConfigs.NSDB_DEFAULT_VALUE).`type` shouldBe Type.STRING
    configKeys.get(NsdbConfigs.NSDB_DEFAULT_VALUE).defaultValue shouldBe null
    configKeys.get(NsdbConfigs.NSDB_DEFAULT_VALUE).importance shouldBe Importance.MEDIUM
    configKeys.get(NsdbConfigs.NSDB_DEFAULT_VALUE).documentation shouldBe NsdbConfigs.NSDB_DEFAULT_VALUE_DOC

    configKeys.get(NsdbConfigs.NSDB_METRIC_RETENTION_POLICY).name shouldBe NsdbConfigs.NSDB_METRIC_RETENTION_POLICY
    configKeys.get(NsdbConfigs.NSDB_METRIC_RETENTION_POLICY).`type` shouldBe Type.STRING
    configKeys.get(NsdbConfigs.NSDB_METRIC_RETENTION_POLICY).defaultValue shouldBe null
    configKeys.get(NsdbConfigs.NSDB_METRIC_RETENTION_POLICY).importance shouldBe Importance.MEDIUM
    configKeys
      .get(NsdbConfigs.NSDB_METRIC_RETENTION_POLICY)
      .documentation shouldBe NsdbConfigs.NSDB_METRIC_RETENTION_POLICY_DOC
    configKeys.get(NsdbConfigs.NSDB_METRIC_RETENTION_POLICY).group shouldBe "Metric Init Params"
    configKeys.get(NsdbConfigs.NSDB_METRIC_RETENTION_POLICY).orderInGroup shouldBe 1
    configKeys.get(NsdbConfigs.NSDB_METRIC_RETENTION_POLICY).width shouldBe ConfigDef.Width.MEDIUM
    configKeys
      .get(NsdbConfigs.NSDB_METRIC_RETENTION_POLICY)
      .displayName shouldBe NsdbConfigs.NSDB_METRIC_RETENTION_POLICY

    configKeys.get(NsdbConfigs.NSDB_SHARD_INTERVAL).name shouldBe NsdbConfigs.NSDB_SHARD_INTERVAL
    configKeys.get(NsdbConfigs.NSDB_SHARD_INTERVAL).`type` shouldBe Type.STRING
    configKeys.get(NsdbConfigs.NSDB_SHARD_INTERVAL).defaultValue shouldBe null
    configKeys.get(NsdbConfigs.NSDB_SHARD_INTERVAL).importance shouldBe Importance.MEDIUM
    configKeys.get(NsdbConfigs.NSDB_SHARD_INTERVAL).documentation shouldBe NsdbConfigs.NSDB_SHARD_INTERVAL_DOC
    configKeys.get(NsdbConfigs.NSDB_SHARD_INTERVAL).group shouldBe "Metric Init Params"
    configKeys.get(NsdbConfigs.NSDB_SHARD_INTERVAL).orderInGroup shouldBe 2
    configKeys.get(NsdbConfigs.NSDB_SHARD_INTERVAL).width shouldBe ConfigDef.Width.MEDIUM
    configKeys.get(NsdbConfigs.NSDB_SHARD_INTERVAL).displayName shouldBe NsdbConfigs.NSDB_SHARD_INTERVAL

  }
}
