###spark整合kafka

实时计算相关技术

|Strom / JStrom|Spark Streming|Flink |
|---|----|-----|    
| 实时性高  |有延迟 |实时性高 |
| 吞吐量较低|吞吐量高|吞吐量高|
|只能实时计算|离线+实时|离线+实时|
|算子比较少|算子丰富|算子丰富|
|没有|机器学习|没有|
|没有|图计算|没有|
|使用比较少|非常火|一般|

一个完整的生态是非常重要的，spark生态特别完善


---------------------------------------------------------

Kafka的一些概念  
	Broker ： 安装Kafka服务的那台集群就是一个broker（broker的id要全局唯一）  
	Producer ：消息的生产者，负责将数据写入到broker中（push）  
	Consumer：消息的消费者，负责从kafka中读取数据（pull），老版本的消费者需要依赖zk，新版本的不需要  
	Topic: 主题，相当于是数据的一个分类，不同topic存放不同的数据  
	Consumer Group： 消费者组，一个topic可以有多个消费者同时消费，多个消费者如果在一个消费者组中，那么他们不能重复消费数据  

Spark Streaming 2.2.0兼容kafka 0.8.2.1以上的版本，主要支持0.8和0.10这两个版本

---------------------------------------------------------
kafka0.8集群安装
	1.下载Kafka安装包  
	2.上传安装包  
	3.解压  
	4.修改配置文件 config/server.properties  
		broker.id=0  
		host.name=node-4  
		log.dirs=/data/kafka  
		zookeeper.connect=node-1:2181,node-2:2181,node-3:2181  
	5.将配置好的kafka拷贝到其他机器上  
	6.修改broker.id和host.name  
	7.启动kafka  
		/bigdata/kafka_2.11-0.8.2.2/bin/kafka-server-start.sh -daemon /bigdata/kafka_2.11-0.8.2.2/config/server.properties   

```bash
	#查看topic信息
	/bigdata/kafka_2.11-0.8.2.2/bin/kafka-topics.sh --list --zookeeper node-1:2181,node-2:2181

	#创建topic
	/bigdata/kafka_2.11-0.8.2.2/bin/kafka-topics.sh --create --zookeeper node-1:2181,node-2:2181 --replication-factor 3 --partitions 3 --topic xiaoniu

	#往Kafka的topic中写入数据(命令行的生成者)
	/bigdata/kafka_2.11-0.8.2.2/bin/kafka-console-producer.sh --broker-list node-4:9092,node-5:9092,node-5:9092 --topic xiaoniu

	#启动消费者
	/bigdata/kafka_2.11-0.8.2.2/bin/kafka-console-consumer.sh --zookeeper node-1:2181,node-2:2181 --topic xiaoniu --from-beginning
```
----

```
kafka1.0集群部署

broker.id=1
delete.topic.enable=true
log.dirs=/bigdata/kafka_2.11-0.10.2.1/data
zookeeper.connect=node-1.xiaoniu.com:2181,node-2.xiaoniu.com:2181,node-3.xiaoniu.com:2181

启动kafka
/bigdata/kafka_2.11-0.10.2.1/bin/kafka-server-start.sh -daemon /bigdata/kafka_2.11-0.10.2.1/config/server.properties 

停止kafka
/bigdata/kafka_2.11-0.10.2.1/bin/kafka-server-stop.sh 


创建topic
/bigdata/kafka_2.11-0.10.2.1/bin/kafka-topics.sh --create --zookeeper node-1.xiaoniu.com:2181,node-2.xiaoniu.com:2181,node-3.xiaoniu.com:2181 --replication-factor 3 --partitions 3 --topic my-topic


列出所有topic
/bigdata/kafka_2.11-0.10.2.1/bin/kafka-topics.sh --list --zookeeper node-1.xiaoniu.com:2181,node-2.xiaoniu.com:2181,node-3.xiaoniu.com:2181

查看某个topic信息
/bigdata/kafka_2.11-0.10.2.1/bin/kafka-topics.sh --describe --zookeeper node-1.xiaoniu.com:2181,node-2.xiaoniu.com:2181,node-3.xiaoniu.com:2181 --topic my-topic

启动一个命令行的生产者
/bigdata/kafka_2.11-0.10.2.1/bin/kafka-console-producer.sh --broker-list node-1.xiaoniu.com:9092,node-1.xiaoniu.xom:9092,node-3.xiaoniu.com:9092 --topic xiaoniu

启动一个命令行的消费者
/bigdata/kafka_2.11-0.10.2.1/bin/kafka-console-consumer.sh --zookeeper node-1.xiaoniu.com:2181,node-2.xiaoniu.com:2181,node-3.xiaoniu.com:2181 --topic my-topic --from-beginning

# 消费者连接到borker的地址
/bigdata/kafka_2.11-0.10.2.1/bin/kafka-console-consumer.sh --bootstrap-server node-1.xiaoniu.com:9092,node-2.xiaoniu.com:9092,node-3.xiaoniu.com:9092 --topic xiaoniu --from-beginning 
```