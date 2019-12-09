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

/**
  * Sink configuration parameters
  */
object NSDbConfigs {

  val NSDB_HOST         = "nsdb.host"
  val NSDB_HOST_DOC     = "Nsdb host"
  val NSDB_HOST_DEFAULT = "localhost"

  val NSDB_PORT         = "nsdb.port"
  val NSDB_PORT_DOC     = "Nsdb port"
  val NSDB_PORT_DEFAULT = 7817

  val NSDB_KCQL     = "nsdb.kcql"
  val NSDB_KCQL_DOC = "Kcql semicolon separated list (mutually exclusive with nsdb.mapping.metrics)"

  val NSDB_DB     = "nsdb.db"
  val NSDB_DB_DOC = "NSDb db (optional)"

  val NSDB_NAMESPACE     = "nsdb.namespace"
  val NSDB_NAMESPACE_DOC = "NSDb namespace (optional)"

  val NSDB_DEFAULT_VALUE     = "nsdb.defaultValue"
  val NSDB_DEFAULT_VALUE_DOC = "NSDb default value (optional)"

  val NSDB_METRIC_RETENTION_POLICY     = "nsdb.metric.retention.policy"
  val NSDB_METRIC_RETENTION_POLICY_DOC = "NSDb retention policy (optional)"

  val NSDB_SHARD_INTERVAL     = "nsdb.shard.interval"
  val NSDB_SHARD_INTERVAL_DOC = "NSDb shard interval (optional)"

  val NSDB_TIMEOUT         = "nsdb.timeout"
  val NSDB_TIMEOUT_DOC     = "Timeout used for test NSDb connection"
  val NSDB_TIMEOUT_DEFAULT = "10 seconds"

  val NSDB_SEMANTIC_DELIVERY         = "nsdb.semantic.delivery"
  val NSDB_SEMANTIC_DELIVERY_DOC     = "NSDb semantic delivery (optional) [at_most_once (default), at_least_once]"
  val NSDB_SEMANTIC_DELIVERY_DEFAULT = Constants.AtMostOnce.value

  val NSDB_AT_LEAST_ONCE_RETRIES         = "nsdb.at.least.once.retries"
  val NSDB_AT_LEAST_ONCE_RETRIES_DOC     = "Number of writing retries when AT_LEAST_ONCE semantic is set"
  val NSDB_AT_LEAST_ONCE_RETRIES_DEFAULT = 10

  val NSDB_AT_LEAST_ONCE_RETRY_INTERVAL         = "nsdb.at.least.once.retry.interval"
  val NSDB_AT_LEAST_ONCE_RETRY_INTERVAL_DOC     = "Time to sleep from a retry to another when AT_LEAST_ONCE semantic is set"
  val NSDB_AT_LEAST_ONCE_RETRY_INTERVAL_DEFAULT = "500 milliseconds"

  val NSDB_MAPPING_METRICS = "nsdb.mapping.metrics"
  val NSDB_MAPPING_METRICS_DOC =
    "Metrics value for mapping configuration in dotted notation (mutually exclusive with nsdb.kcql)"

  val NSDB_MAPPING_VALUES     = "nsdb.mapping.values"
  val NSDB_MAPPING_VALUES_DOC = "Values value for mapping configuration in dotted notation (optional)"

  val NSDB_MAPPING_TAGS     = "nsdb.mapping.tags"
  val NSDB_MAPPING_TAGS_DOC = "Tags value for mapping configuration in dotted notation (optional)"

  val NSDB_MAPPING_TIMESTAMPS     = "nsdb.mapping.timestamps"
  val NSDB_MAPPING_TIMESTAMPS_DOC = "Timestamps value for mapping configuration in dotted notation (optional)"

  // inner property
  val NSDB_INNER_ENCODED_MAPPINGS_TYPE  = "nsdb.inner.encoded.queries.type"
  val NSDB_INNER_ENCODED_MAPPINGS_VALUE = "nsdb.inner.encoded.queries.value"

  /**
    * @return sink expected configuration:
    *
    *         - nsdb.host nsdb hosts.
    *
    *         - nsdb.port nsdb port.
    *
    *         - nsdb.kcql semicolon separated list of kcql statements to filter data from topics.
    *
    *         - nsdb.db the nsdb db to store records in case a mapping in the kcql is not defined.
    *
    *         - nsdb.namespace the nsdb db to store records in case a mapping in the kcql is not defined.
    *
    *         - nsdb.defaultValue the value to be used in case a mapping in the kcql is not defined.
    *
    *         - nsdb.metric.retention.policy the retention policy applied to the metric specified in the kcql.
    *
    *         - nsdb.shard.interval the shard interval applied to the metric specified in the kcql.
    *
    *         - nsdb.timeout timeout used for test nsdb connection
    *
    *         - nsdb.semantic.delivery the semantic delivery for writing data into nsdb
    *
    *         - nsdb.at.least.once.retries number of maximum writing retries
    *
    *         - nsdb.at.least.once.retry.interval time to sleep from a retry to another
    *
    *         - nsdb.mapping.metrics colon separated list of metrics
    *
    *         - nsdb.mapping.values colon separated list of values
    *
    *         - nsdb.mapping.tags colon separated list of tags
    *
    *         - nsdb.mapping.timestamps colon separated list of timestamps
    */
  def configDef: ConfigDef =
    new ConfigDef()
      .define(NSDB_HOST,
              Type.STRING,
              NSDB_HOST_DEFAULT,
              Importance.HIGH,
              NSDB_HOST_DOC,
              "Connection",
              1,
              ConfigDef.Width.MEDIUM,
              NSDB_HOST)
      .define(NSDB_PORT,
              Type.INT,
              NSDB_PORT_DEFAULT,
              Importance.MEDIUM,
              NSDB_PORT_DOC,
              "Connection",
              2,
              ConfigDef.Width.MEDIUM,
              NSDB_PORT)
      .define(NSDB_KCQL,
              Type.STRING,
              null,
              Importance.HIGH,
              NSDB_KCQL_DOC,
              "Connection",
              3,
              ConfigDef.Width.MEDIUM,
              NSDB_KCQL)
      .define(NSDB_DB, Type.STRING, null, Importance.MEDIUM, NSDB_DB_DOC)
      .define(NSDB_NAMESPACE, Type.STRING, null, Importance.MEDIUM, NSDB_NAMESPACE_DOC)
      .define(NSDB_DEFAULT_VALUE, Type.STRING, null, Importance.MEDIUM, NSDB_DEFAULT_VALUE_DOC)
      .define(
        NSDB_METRIC_RETENTION_POLICY,
        Type.STRING,
        null,
        Importance.MEDIUM,
        NSDB_METRIC_RETENTION_POLICY_DOC,
        "Metric Init Params",
        1,
        ConfigDef.Width.MEDIUM,
        NSDB_METRIC_RETENTION_POLICY
      )
      .define(NSDB_SHARD_INTERVAL,
              Type.STRING,
              null,
              Importance.MEDIUM,
              NSDB_SHARD_INTERVAL_DOC,
              "Metric Init Params",
              2,
              ConfigDef.Width.MEDIUM,
              NSDB_SHARD_INTERVAL)
      .define(NSDB_TIMEOUT,
              Type.STRING,
              NSDB_TIMEOUT_DEFAULT,
              Importance.MEDIUM,
              NSDB_TIMEOUT_DOC,
              "Connection",
              4,
              ConfigDef.Width.MEDIUM,
              NSDB_TIMEOUT)
      .define(
        NSDB_SEMANTIC_DELIVERY,
        Type.STRING,
        NSDB_SEMANTIC_DELIVERY_DEFAULT,
        ConfigDef.ValidString.in(Constants.AtMostOnce.value, Constants.AtLeastOnce.value),
        Importance.MEDIUM,
        NSDB_SEMANTIC_DELIVERY_DOC,
        "Semantic Delivery",
        1,
        ConfigDef.Width.MEDIUM,
        NSDB_SEMANTIC_DELIVERY
      )
      .define(
        NSDB_AT_LEAST_ONCE_RETRIES,
        Type.INT,
        NSDB_AT_LEAST_ONCE_RETRIES_DEFAULT,
        Importance.LOW,
        NSDB_AT_LEAST_ONCE_RETRIES_DOC,
        "Semantic Delivery",
        2,
        ConfigDef.Width.MEDIUM,
        NSDB_AT_LEAST_ONCE_RETRIES
      )
      .define(
        NSDB_AT_LEAST_ONCE_RETRY_INTERVAL,
        Type.STRING,
        NSDB_AT_LEAST_ONCE_RETRY_INTERVAL_DEFAULT,
        Importance.LOW,
        NSDB_AT_LEAST_ONCE_RETRY_INTERVAL_DOC,
        "Semantic Delivery",
        3,
        ConfigDef.Width.MEDIUM,
        NSDB_AT_LEAST_ONCE_RETRY_INTERVAL
      )
      .define(
        NSDB_MAPPING_METRICS,
        Type.STRING,
        null,
        DottedNotationValidator,
        Importance.HIGH,
        NSDB_MAPPING_METRICS_DOC,
        "Mapping Configuration",
        1,
        ConfigDef.Width.MEDIUM,
        NSDB_MAPPING_METRICS
      )
      .define(
        NSDB_MAPPING_VALUES,
        Type.STRING,
        null,
        DottedNotationValidator,
        Importance.HIGH,
        NSDB_MAPPING_VALUES_DOC,
        "Mapping Configuration",
        2,
        ConfigDef.Width.MEDIUM,
        NSDB_MAPPING_VALUES
      )
      .define(
        NSDB_MAPPING_TAGS,
        Type.STRING,
        null,
        DottedNotationValidator,
        Importance.HIGH,
        NSDB_MAPPING_TAGS_DOC,
        "Mapping Configuration",
        3,
        ConfigDef.Width.MEDIUM,
        NSDB_MAPPING_TAGS
      )
      .define(
        NSDB_MAPPING_TIMESTAMPS,
        Type.STRING,
        null,
        DottedNotationValidator,
        Importance.HIGH,
        NSDB_MAPPING_TIMESTAMPS_DOC,
        "Mapping Configuration",
        4,
        ConfigDef.Width.MEDIUM,
        NSDB_MAPPING_TIMESTAMPS
      )
}
