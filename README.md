# pipe-s

Pipe-s is a common ETL pipeline built on [go-disruptor](https://github.com/smarty-prototypes/go-disruptor) 
and replicates data between various inputs and outputs such as mysql and kafka. It has similar functions
like [gravity](https://github.com/moiot/gravity) and good performance and extendability.

# Usage

## as a binary
```bash
git clone https://github.com/singular-seal/pipe-s.git
cd pipe-s && make
```
Modify the configuration [sample](https://github.com/singular-seal/pipe-s/blob/main/examples/configsamples/hello_world.json) 
according to your environment.
```bash
bin/task --config examples/configsamples/hello_world.json
```

## as a lib
to do

# Features

## input support
* Mysql binlog
* Mysql scan
* Kafka

## output support
* Mysql stream
* Mysql batch
* Mysql data check
* Kafka
* Log

## data transformation
* Filter data by matching db and table names with regular expression.
* Db, table, column and operation mapping by configuration.
* More flexible transformer customized by yourself.

# Documents
* [architecture](docs/arch.md)
* [config](docs/config.md)
* [performance](docs/performance.md)

# Contacts
Send message to [me](mailto:singular.seal@gmail.com) if you have any questions to discuss.  
