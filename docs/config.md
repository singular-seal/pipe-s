# Examples
### mysql binlog to mysql
* [stream sync](../examples/configsamples/db_stream_sync.json), write to destination as stream, lower throughput and latency.
* [batch sync](../examples/configsamples/db_batch_sync.json), write to destination as batches, higer throughput and latency and has a known data loss scenario.

### data consistency check between 2 mysql databases
* [db check](../examples/configsamples/db_check.json), statically read data from source db batch by batch and compare them to the destination db.

### mysql binlog to kafka
* [binlog kafka](../examples/configsamples/binlog_kafka.json), using zookeeper as the state store.

### kafka to mysql
* [kafka mysql](../examples/configsamples/kafka_mysql.json)
