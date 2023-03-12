# General Ideas
* Build on disruptor and try to make as many places as possible running in parallel.
* Everything is a component and configurable, users can compose data processing logic by configuration.

# Is go-disruptor Safe?
There's a description in go-disruptor - *This code is currently experimental and is not recommended for production environments.*
But on June 6, 2022, there's an update on [go memory model](https://go.dev/ref/mem) and there's the 
description on atomic values - *The preceding definition has the same semantics as C++’s sequentially consistent atomics and Java’s volatile variables.*
go-disruptor is using atomic operations to ensure visibility between different goroutines, so it should be safe under the updated go memory model.

# Data Consistency Guarantee
'Data Loss' is a common worry on data replication pipelines. So I made a [test case](https://github.com/singular-seal/pipe-s/tree/main/integration_test/dbsync)
which runs constantly to detect possible data loss scenarios. 