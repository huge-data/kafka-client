
## 消息队列客户端

> 基于confluent-2.0.1或者Kafka-11-0.9.0.0实现。

### 测试脚本

#### 查看所有Topic

```
$ bin/kafka-topics --zookeeper kafka01:2181,kafka02:2181,kafka03:2181/confluent --list
```

### 查看某个Topic详情

```
$ bin/kafka-topics --zookeeper kafka01:2181,kafka02:2181,kafka03:2181/confluent --topic apt-cache --describe
```

#### 创建Topic

```
$ bin/kafka-topics.sh --zookeeper kafka01:2181,kafka02:2181,kafka03:2181/confluent --create --topic apt-test --partitions 24 --replication-factor 1
```

#### 生产数据

```
$ bin/kafka-producer-perf-test.sh --topic apt-test --num-records 50000000 --record-size 10000 --throughput 10000000 --producer-props bootstrap.servers=kafka01:9092,kafka02:9092,kafka03:9092 acks=0 batch.size=81960
```

#### 查看数据

```
$ bin/kafka-console-consumer.sh --zookeeper kafka01:2181,kafka02:2181,kafka03:2181/kafka --from-beginning --topic apt-test
```

> 注意：在测试机器上需要绑定Kafka机器的ip和主机名。

### 查看流量

`dstat命令`： dstat -nf

`iptraf命令`： iptraf

### 测试结果

> 在三台24核、SSD、64G内存、万兆网卡的机器上测试每台机器峰值流量可以达到550M/s，三台机器合计总量可以超过1GB/s。

### 注意事项

1. 在kafka集群中每台机器必须绑定各自的主机名。

2. 在本地调试时，也需要将机器的ip和主机名绑定。

3. Topic的名字中最好不要出现'.'或者'_'，以免和Metric名称混淆。