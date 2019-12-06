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

package io.radicalbit.nsdb.connector.kafka.sink

import java.util.{Collection => JCollection, Map => JMap}

import io.radicalbit.nsdb.api.scala.NSDB
import io.radicalbit.nsdb.connector.kafka.sink.NSDbSinkWriter._
import io.radicalbit.nsdb.connector.kafka.sink.conf.{NSDbConfigs, QueryConfUtility}
import org.apache.kafka.common.utils.AppInfoParser
import org.apache.kafka.connect.sink.{SinkRecord, SinkTask}
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.concurrent.{Await, ExecutionContext}

/**
  * NSDb Sink task.
  */
class NSDbSinkTask extends SinkTask with QueryConfUtility {
  private val log = LoggerFactory.getLogger(classOf[NSDbSinkTask])

  private var writer: Option[NSDbSinkWriter] = None

  /**
    * Opens a new connection against Nsdb target and setup the writer.
    **/
  override def start(props: JMap[String, String]): Unit = {
    log.info("Starting a {} task.", classOf[NSDbSinkTask].getSimpleName)
    log.info("Properties are {}.", props)

    import scala.concurrent.duration._

    val timeout =
      validateDuration(NSDbConfigs.NSDB_TIMEOUT, Option(props.get(NSDbConfigs.NSDB_TIMEOUT)))
        .getOrElse(Duration(NSDbConfigs.NSDB_TIMEOUT_DEFAULT))
    val retries = Option(props.get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRIES).toInt)
      .getOrElse(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRIES_DEFAULT)
    val retryInterval = validateDuration(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL,
                                         Option(props.get(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL)))
      .getOrElse(Duration(NSDbConfigs.NSDB_AT_LEAST_ONCE_RETRY_INTERVAL_DEFAULT))

    writer = Some(
      new NSDbSinkWriter(
        // NOTE: it's fine to directly convert port into integer since props are validated when connector is deployed
        connection = Await.result(NSDB.connect(props.get(NSDbConfigs.NSDB_HOST),
                                               props.get(NSDbConfigs.NSDB_PORT).toInt)(ExecutionContext.global),
                                  timeout),
        parsedKcql = function(props),
        globalDb = Option(props.get(NSDbConfigs.NSDB_DB)),
        globalNamespace = Option(props.get(NSDbConfigs.NSDB_NAMESPACE)),
        defaultValue = validateDefaultValue(Option(props.get(NSDbConfigs.NSDB_DEFAULT_VALUE))),
        retentionPolicy = validateDuration(NSDbConfigs.NSDB_METRIC_RETENTION_POLICY,
                                           Option(props.get(NSDbConfigs.NSDB_METRIC_RETENTION_POLICY))),
        shardInterval =
          validateDuration(NSDbConfigs.NSDB_SHARD_INTERVAL, Option(props.get(NSDbConfigs.NSDB_SHARD_INTERVAL))),
        semanticDelivery =
          validateSemanticDelivery(NSDbConfigs.NSDB_SEMANTIC_DELIVERY, props.get(NSDbConfigs.NSDB_SEMANTIC_DELIVERY)),
        retries = retries,
        retryInterval = retryInterval,
        timeout = timeout
      ))
  }

  /**
    * Forwards the SinkRecords to the writer for writing.
    **/
  override def put(records: JCollection[SinkRecord]): Unit = {
    writer.foreach(w => w.write(records.asScala.toList))
  }

  override def stop(): Unit = writer = None

  override def version(): String = AppInfoParser.getVersion

}
