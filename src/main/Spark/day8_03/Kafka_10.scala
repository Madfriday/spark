package day8_03

import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.SparkConf
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010._



import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
 * Created by zx on 2017/7/6.
 */
object DirectStream {

  def main(args: Array[String]): Unit = {

    val group = "g1"
    val topic = "recharge"
    //创建SparkConf，如果将任务提交到集群中，那么要去掉.setMaster("local[2]")
    val conf = new SparkConf().setAppName("DirectStream").setMaster("local[2]")
    //创建一个StreamingContext，其里面包含了一个SparkContext
    val streamingContext = new StreamingContext(conf, Seconds(5));

    //配置kafka的参数
    val kafkaParams = Map[String, Object](
      "bootstrap.servers" -> "node1:9092,slave1:9092,slave2:9092",
      "key.deserializer" -> classOf[StringDeserializer],
      "value.deserializer" -> classOf[StringDeserializer],
      "group.id" -> group,
      "partition.assignment.strategy" -> "org.apache.kafka.clients.consumer.RangeAssignor",
      "auto.offset.reset" -> "latest", // lastest
      "enable.auto.commit" -> (true: java.lang.Boolean)
    )

    val topics = Array(topic)
    //在Kafka中记录读取偏移量
    val stream = KafkaUtils.createDirectStream[String, String](
      streamingContext,
      //位置策略
      PreferConsistent,
      //订阅的策略
      Subscribe[String, String](topics, kafkaParams)
    )


    //迭代DStream中的RDD，将每一个时间点对于的RDD拿出来
    stream.foreachRDD { rdd =>
      if (!rdd.isEmpty()) {
        //获取该RDD对于的偏移量
        val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        //拿出对于的数据，foreach是一个aciton
        val re1 = rdd.map(x =>{
          val con: Array[String] = x.value().split(",")
          val re = new Bikes(con(0),con(1),con(2),con(3).toInt,con(4).toDouble,con(5).toDouble,con(6),con(7),con(8))
        })
          println(re1)

        //更新偏移量
        // some time later, after outputs have completed
        stream.asInstanceOf[CanCommitOffsets].commitAsync(offsetRanges)
      }
    }
    streamingContext.start()
    streamingContext.awaitTermination()
  }
  case class Bikes(val time:String,val phone : String, val types:String,val amount:Int,val lat:Double,val log:Double,val por:String,
                   val city:String,val district:String) {
    override def toString: String = {
      s"$time,$phone,$types"
    }
  }
}
