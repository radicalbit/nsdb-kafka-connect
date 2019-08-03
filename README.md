[![Build Status](https://travis-ci.org/radicalbit/nsdb-kafka-connect.svg)](https://travis-ci.org/radicalbit/nsdb-kafka-connect)
[![codecov](https://codecov.io/github/radicalbit/nsdb-kafka-connect/coverage.svg?branch=master)](https://codecov.io/github/radicalbit/nsdb-kafka-connect?branch=master)
[![License](https://img.shields.io/github/license/radicalbit/nsdb-kafka-connect.svg)](LICENSE)

# NSDb Kafka Connect #

NSDb is a **time-series** database **streaming oriented**
optimized for the serving layer.

This repo contains the connector for sinking data into NSDb using kafka connect.

NSDb Kafka Sink allows you to write events from Kafka to Nsdb.
The connector takes the value from the Kafka Connect SinkRecords and inserts a new bit into NSDb.
Prerequisites

- Apache Kafka 0.10.x or above
- Kafka Connect 0.10.x or above
- NSDb 0.7.0 or above

# Configurations

## General Connector Configuration
The Kafka Connect framework requires the following in addition to any connectors specific configurations:

Name  | Description  | Type  | Value
--|---|---|--
name  |  Name of the connector | String  |  Anything unique across the Connect cluster
topics  | The topics to sink | String | comma separated list of topics used in the connector
tasks.max  | The number of tasks to be created across the connect cluster  | Int | Default value is 1
connector.class  | Connector FQCN  |  String | io.radicalbit.nsdb.connector.kafka.sink.NSDbSinkConnector

## Specific Nsdb Sink Configuration
Name  | Description  | Type  | Value
--|---|---|--
nsdb.host  | Hostname of the NSDb instance to connect to | String | default value is `localhost`
nsdb.port  | Port of the NSDb instance to connect to | Int | default value is `7817`
nsdb.kcql  | Kcql expressions used to map topic data to NSDb bits | String  | semicolon separated Kcql expressions
nsdb.db  | NSDb db to use in case no mappig is provided in the Kcql | String  |  If a mapping is provided in the Kcql this config will be overridden
nsdb.namespace  | NSDb db to use in case no mappig is provided in the Kcql | String  | If a mapping is provided in the Kcql this config will be overridden
nsdb.defaultValue | default value | Numeric | if a value alias is provided in the Kcql expression this config will be ignored
nsdb.metric.retention.policy | NSDb custom retention policy | String | Custom NSDb retention policy applied to the metric specified in the Kcql statements formatted as a Scala Duration (e.g. 2 d, 2d, 2 days).<br>If this configuration is not provided, no retention policy will be applied to the metrics. 
nsdb.shard.interval | NSDb custom shard interval policy | String | NSDb shard interval applied to the metric specified in the Kcql statements formatted as a Scala Duration (e.g. 2 d, 2d, 2 days).<br>If this configuration is not provided, the default shard interval will be applied to the metrics.

## KCQL Support

The NSDb sink supports KCQL, [Kafka Connect Query Language](https://github.com/Landoop/kafka-connect-query-language).

The following capabilites can be achieved using KCQL:

- Dimensions and tags selection and mapping
- Tags selection among the mapping above
- Value mapping
- Timestamp Mapping
- Target bit selection
- Target db and namespace selection (if not specified, global configurations will be used)

```sql
-- Select field x as dimension a, field y as value and z as the timestamp from topicA to bitA
INSERT INTO bitA SELECT x AS a, y AS value FROM topicA WITHTIMESTAMP z

-- Select field x as dimension a, field z as dimension c, field y as value and the current time as the timestamp from topicB to bitB
INSERT INTO bitB SELECT x AS a, y AS value, z as c FROM topicB WITHTIMESTAMP sys_time()

-- Select field d as the db, field n as the namespace, field x as tag a, field z as tag b, field t as dimension c, field y as value and the current time as the timestamp from topicC to bitC
INSERT INTO bitC SELECT d as db, n as namespace, x AS a, y AS value, z as b, t as c FROM topicC WITHTIMESTAMP sys_time() WITHTAG(a,b)
```

## example
```bash
echo '{"name":"manufacturing-nsdb-sink",
"config": {"connector.class":"io.radicalbit.nsdb.connector.kafka.sink.NSDbSinkConnector",
"tasks.max":"1","nsdb.host":"nsdbhost",
"topics":"topicA, topicB, topicC",
"nsdb.metric.retention.policy": "2d",
"nsdb.shard.interval": "2d",
"nsdb.kcql":
"INSERT INTO bitA SELECT x AS a, y AS value FROM topicA WITHTIMESTAMP z;
 INSERT INTO bitB SELECT x AS a, y AS value, z as c FROM topicB WITHTIMESTAMP sys_time();
 INSERT INTO bitC SELECT d as db, n as namespace, x AS a, y AS value, z as b, t as c FROM topicC WITHTIMESTAMP sys_time() WITHTAG(a,b)"
}}' | curl -X POST -d @- http://kafkaconnecthost:8083/connectors --header "content-Type:application/json"
```


## License

NSDb is distributed under the [Apache 2](http://www.apache.org/licenses/LICENSE-2.0) license.