Pipe-s是基于[go-disruptor](https://github.com/smarty-prototypes/go-disruptor)构建的数据同步管道，
可以用于多种数据源之间的数据同步，比如mysql和kafka。Pipe-s在功能上类似于[gravity](https://github.com/moiot/gravity).

# 为什么用pipe-s?

Pipe-s的主要特点是高性能和高扩展性

### 性能测试结果

硬件： i5-12600K cpu, 32G 内存, WD black nvme ssd  
操作系统： ubuntu 22.04  
软件栈： mysql 8.0.32, kafka 2.4.0  
源和目标mysql以及kafka都装在同一台物理机上，但是源和目标使用不同的ssd。  
测试流量： 基于sysbench mysql相关场景产生的标准流量。  

| 流量类型          | 场景                    | 网络流量  | tps  | 其它说明                                                               |
|---------------|-----------------------|-------|------|--------------------------------------------------------------------|
| 只有insert的大事务  | go-mysql              | 280MB | -    | [go-mysql](https://github.com/go-mysql-org/go-mysql) 1.7.0 拿到事件即丢弃 |
| 只有insert的大事务  | pipe-s                | 400MB | -    | 深度解析，DummyOutput                                                   |
| 只有insert的大事务  | DisruptorBinlogSyncer | 1.4GB | -    | DisruptorBinlogSyncer是pipe-s的binlog解析组件，拿到事件即丢弃                    |
| 只有insert的单条事务 | go-mysql              | 68MB  | -    | go-mysql 1.7.0 拿到事件即丢弃                                             |
| 只有insert的单条事务 | pipe-s(filepos mode)  | 270MB | -    | 深度解析，DummyOutput                                                   |
| 只有insert的单条事务 | pipe-s(gtid mode)     | 228MB | -    | 深度解析， DummyOutput                                                  |
| 只有insert的单条事务 | pipe-s 到 kafka        | 200MB | 400K | gtid mode                                                          |
| 只有insert的单条事务 | pipe-s 到 mysql stream | 35MB  | 70K  | gtid mode， 流式同步                                                    |
| 只有insert的单条事务 | pipe-s 到 mysql batch  | 70MB  | 140K | gtid mode， 批量同步                                                    |
| 增删改的单条事务      | pipe-s 到 mysql steam  | 23MB  | 46K  | gtid mode， 流式同步                                                    |
| 增删改的单条事务      | pipe-s 到 mysql batch  | 65MB  | 130K | gtid mode， 批量同步                                                    |

### 扩展性

Pipe-s的设计是所有的功能都是组件-可配置可扩展。组件包括输入，输出，处理器，状态存储，用户也可以实现并注册自己的组件。

# 怎样使用？

### 用作二进制程序

```bash
git clone https://github.com/singular-seal/pipe-s.git
cd pipe-s && make
```

根据你自己的环境修改配置样例
[样例](https://github.com/singular-seal/pipe-s/blob/main/examples/configsamples/hello_world.json)

```bash
bin/task --config examples/configsamples/hello_world.json
```

### 用作golang lib

to do

# 支持的功能特性

### 输入组件

* Mysql binlog
* Mysql scan
* Kafka

### 输出组件

* Mysql stream
* Mysql batch
* Mysql data check
* Kafka
* Log

### 数据转换支持

* 根据库名表名的正则表达式过滤数据。
* 根据配置做库名，表名，列明和操作映射。
* 自定义代码转换。

# 文档

* [技术架构](docs/arch.md)
* [配置样例](docs/config.md)

# 联系方式

Gmail: singular.seal@gmail.com，欢迎反馈。  
