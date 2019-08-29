//package day8_02
//
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//import org.apache.spark.streaming.kafka.KafkaUtils
//
//object Demo02 {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("Demo02").setMaster("local[2]")
//    val ssc = new StreamingContext(conf, Seconds(5))
//    val zkQuorum = "node1:2181,slave1:2181,slave2:2181"
//    val groupId = "g100"
//    val topic = Map[String, Int]("you" -> 1)
//    val data = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)
//    val lines = data.map(_._2)
//    lines.map((_, 1)).reduceByKey(_ + _).print()
//    ssc.start()
//    ssc.awaitTermination()
//  }
//
//}
