# **pipe-s** [中文](./README-cn.md)

[![Go Report Card](https://goreportcard.com/badge/github.com/singular-seal/pipe-s)](https://goreportcard.com/report/github.com/singular-seal/pipe-s)

Pipe-s is a data replication pipeline built on [go-disruptor](https://github.com/smarty-prototypes/go-disruptor)
and replicates data between various inputs and outputs such as mysql and kafka. It has similar functions
like [gravity](https://github.com/moiot/gravity).

# Why pipe-s?

High performance and extensibility.

### Benchmark

Hardware: i5-12600K cpu, 32G memory, WD black nvme ssd  
OS: ubuntu 22.04  
Software: mysql 8.0.32, kafka 2.4.0  
Source mysql, dest mysql and kafka are installed on the same machine but different ssd.     
Traffic: Produced by standard sysbench mysql scripts.  

| traffic                                | scenario               | throughput | tps  | description                                                                         |
|----------------------------------------|------------------------|------------|------|-------------------------------------------------------------------------------------|
| big transaction insert only            | go-mysql               | 280MB      | -    | [go-mysql](https://github.com/go-mysql-org/go-mysql) 1.7.0 fetch event then abandon |
| big transaction insert only            | pipe-s                 | 400MB      | -    | deeply resolve event and DummyOutput                                                |
| big transaction insert only            | DisruptorBinlogSyncer  | 1.4GB      | -    | DisruptorBinlogSyncer is pipe-s's component, fetch event then abandon               |
| small transaction insert only          | go-mysql               | 68MB       | -    | go-mysql 1.7.0 just fetch event and abandon                                         |
| small transaction insert only          | pipe-s(filepos mode)   | 270MB      | -    | deep resolve event and DummyOutput                                                  |
| small transaction insert only          | pipe-s(gtid mode)      | 228MB      | -    | deep resolve event and DummyOutput                                                  |
| small transaction insert only          | pipe-s to kafka        | 200MB      | 400K | gtid mode                                                                           |
| small transaction insert only          | pipe-s to mysql stream | 35MB       | 70K  | gtid mode, steam sync                                                               |
| small transaction insert only          | pipe-s to mysql batch  | 70MB       | 140K | gtid mode, batch sync                                                               |
| small transaction insert update delete | pipe-s to mysql steam  | 23MB       | 46K  | gtid mode, steam sync                                                               |
| small transaction insert update delete | pipe-s to mysql batch  | 65MB       | 130K | gtid mode, batch sync                                                               |

### Extensibility

In pipe-s, everything is component which is configurable and extensible, including input, output, processor and state store. 
You can also implement and register your own components.  

# Usage

### as a binary

```bash
git clone https://github.com/singular-seal/pipe-s.git
cd pipe-s && make
```

Modify the
configuration [sample](https://github.com/singular-seal/pipe-s/blob/main/examples/configsamples/hello_world.json)
according to your environment.

```bash
bin/task --config examples/configsamples/hello_world.json
```

### as a lib

to do

# Features

### input support

* Mysql binlog
* Mysql scan
* Kafka

### output support

* Mysql stream
* Mysql batch
* Mysql data check
* Kafka
* Log

### data transformation

* Filter data by matching db and table names with regular expression.
* Db, table, column and operation mapping by configuration.
* More flexible transformer customized by yourself.

# Documents

* [architecture](docs/arch.md)
* [config](docs/config.md)

# Contacts

Gmail: singular.seal@gmail.com, any feedback is welcome.  
