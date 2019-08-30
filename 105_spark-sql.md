##spark 整合hive


1. parkSQL是Spark上的高级模块，SparkSQL是一个SQL解析引擎，将SQL解析成特殊的RDD（DataFrame），然后在Spark集群中运行  

2. SparkSQL是用来处理结构化数据的（先将非结构化的数据转换成结构化数据）

3. SparkSQL支持两种编程API
	1.SQL方式
	2.DataFrame的方式（DSL）

4. SparkSQL兼容hive（元数据库、SQL语法、UDF、序列化、反序列化机制）

5. sparkSQL支持统一的数据源，课程读取多种类型的数据

6. SparkSQL提供了标准的连接（JDBC、ODBC），以后可以对接一下BI工具

------------------------------------------------------------

RDD和DataFrame的区别

DataFrame里面存放的结构化数据的描述信息，DataFrame要有表头（表的描述信息），描述了有多少列，每一列数叫什么名字、什么类型、能不能为空？

DataFrame是特殊的RDD（RDD+Schema信息就变成了DataFrame）


------------------------------------------------------------
SparkSQL的第一个入门程序

首先在pom中添加sparkSQL的依赖

 <!-- 导入spark sql的依赖 -->
<dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>spark-sql_2.11</artifactId>
    <version>${spark.version}</version>
</dependency>

SparkSQL 1.x和2.x的编程API有一些变化，企业中都有使用，所以两种方式都将

先用1.x的方式：
	SQL方式
		创建一个SQLContext
			1.创建sparkContext，然后再创建SQLContext
			2.先创建RDD，对数据进行整理，然后关联case class，将非结构化数据转换成结构化数据
			3.显示的调用toDF方法将RDD转换成DataFrame
			4.注册临时表
			5.执行SQL（Transformation，lazy）
			6.执行Action
			----
			1.创建sparkContext，然后再创建SQLContext
			2.先创建RDD，对数据进行整理，然后关联Row，将非结构化数据转换成结构化数据
			3.定义schema
			4.调用sqlContext的createDataFrame方法
			5.注册临时表
			6.执行SQL（Transformation，lazy）
			7.执行Action
	DSL（DatFrame API）
		



先用2.x的方式：
	创建一个SparkSession
	val spark = SparkSession.builder()
      .appName("DataSetWordCount")
      .master("local[*]")
      .getOrCreate()


----------------------------------------------------

UDF （user defined function）
	UDF    输入一行，返回一个结果    一对一    ip2Province(123123111)   ->  辽宁省
	UDTF   输入一行，返回多行（hive）一对多    spark SQL中没有UDTF，spark中用flatMap即可实现该功能  
	UDAF   输入多行，返回一行 aggregate(聚合) count、sum这些是sparkSQL自带的聚合函数，但是复杂的业务，要自己定义

----------------------------------------------------

Dateset是spark1.6以后推出的新的API，也是一个分布式数据集，于RDD相比，保存了跟多的描述信息，概念上等同于关系型数据库中的二维表，基于保存了跟多的描述信息，spark在运行时可以被优化。

Dateset里面对应的的数据是强类型的，并且可以使用功能更加丰富的lambda表达式，弥补了函数式编程的一些缺点，使用起来更方便

在scala中，DataFrame其实就是Dateset[Row]

Dataset的特点：
	1.一系列分区
	2.每个切片上会有对应的函数
	3.依赖关系
	4.kv类型shuffle也会有分区器
	5.如果读取hdfs中的数据会感知最优位置
	6.会优化执行计划
	7.支持更加智能的数据源


调用Dataset的方法先会生成逻辑计划，然后被spark的优化器进行优化，最终生成物理计划，然后提交到集群中运行！
	
----------------------------------------------------

1.Hive On Spark （跟hive没太的关系，就是使用了hive的标准（HQL， 元数据库、UDF、序列化、反序列化机制））

在公司中使用hive还是非常多的

2.Hive原来的计算模型是MR,有点慢（将中间结果写入到HDFS中）


3.Hive On Spark 使用RDD（DataFrame），然后运行在spark 集群上


4.真正要计算的数据是保存在HDFS中，mysql这个元数据库，保存的是hive表的描述信息，描述了有哪些database、table、以及表有多少列，每一列是什么类型，还要描述表的数据保存在hdfs的什么位置？

5.hive跟mysql的区别:

hive是一个数据仓库（存储数据并分析数据，分析数据仓库中的数据量很大，一般要分析很长的时间）
mysql是一个关系型数据库（关系型数据的增删改查（低延迟））


hive的元数据库中保存怎知要计算的数据吗？
	不保存，保存hive仓库的表、字段、等描述信息

真正要计算的数据保存在哪里了？
	保存在HDFS中了


hive的元数据库的功能
	建立了一种映射关系，执行HQL时，先到MySQL元数据库中查找描述信息，然后根据描述信息生成任务，然后将任务下发到spark集群中执行

###spark sql 整合hive，将hive的sql写在一个文件中执行（用-f这个参数）
/bigdata/spark-2.2.0-bin-hadoop2.7/bin/spark-sql --master spark://node-4:7077,node-5:7077 --driver-class-path /home/xiaoniu/mysql-connector-java-5.1.7-bin.jar -f hive-sqls.sql

在idea中开发，整合hive
 <!-- spark如果想整合Hive，必须加入hive的支持 -->
	<dependency>
	    <groupId>org.apache.spark</groupId>
	    <artifactId>spark-hive_2.11</artifactId>
	    <version>2.2.0</version>
	</dependency>

    //如果想让hive运行在spark上，一定要开启spark对hive的支持
    val spark = SparkSession.builder()
      .appName("HiveOnSpark")
      .master("local[*]")
      .enableHiveSupport()//启用spark对hive的支持(可以兼容hive的语法了)
      .getOrCreate()

------------------------------------------------------

linux上整合
```
1.安装MySQL（hive的元数据库）并创建一个普通用户，并且授权
	CREATE USER 'xiaoniu'@'%' IDENTIFIED BY '123568'; 
	GRANT ALL PRIVILEGES ON hivedb.* TO 'xiaoniu'@'%' IDENTIFIED BY '123568' WITH GRANT OPTION;
	FLUSH PRIVILEGES;


#在spark的conf目录下创建一个hive的配置文件
2.添加一个hive-site.xml

<?xml version="1.0" encoding="UTF-8" standalone="no"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<!--
   Licensed to the Apache Software Foundation (ASF) under one or more
   contributor license agreements.  See the NOTICE file distributed with
   this work for additional information regarding copyright ownership.
   The ASF licenses this file to You under the Apache License, Version 2.0
   (the "License"); you may not use this file except in compliance with
   the License.  You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
-->
<configuration>
  <property>
    <name>javax.jdo.option.ConnectionURL</name>
    <value>jdbc:mysql://node-6:3306/hivedb?createDatabaseIfNotExist=true</value>
    <description>JDBC connect string for a JDBC metastore</description>
  </property>

   <property>
    <name>javax.jdo.option.ConnectionDriverName</name>
    <value>com.mysql.jdbc.Driver</value>
    <description>Driver class name for a JDBC metastore</description>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionUserName</name>
    <value>bigdata</value>
    <description>username to use against metastore database</description>
  </property>

  <property>
    <name>javax.jdo.option.ConnectionPassword</name>
    <value>123568</value>
    <description>password to use against metastore database</description>
  </property>

</configuration>

3.上传一个mysql连接驱动（sparkSubmit也要连接MySQL，获取元数据信息）
./spark-sql --master spark://node-4:7077,node-5:7077 --driver-class-path /home/xiaoniu/mysql-connector-java-5.1.7-bin.jar

4.sparkSQL会在mysql上创建一个database，需要手动改一下DBS表中的DB_LOCATION_UIR改成hdfs的地址

5.要在/etc/profile中配置一个环节变量(让sparkSQL知道hdfs在哪里，其实就是namenode在哪里)
  exprot HADOOP_CONF_DIR

6.重新启动SparkSQL的命令行
```