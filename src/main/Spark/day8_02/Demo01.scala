//package day8_02
//
//import org.apache.spark.SparkConf
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object Demo01 {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("Demo2").setMaster("local[2]")
//    val ssc = new StreamingContext(conf, Seconds(5))
//    val lines = ssc.socketTextStream("192.168.9.11", 8888)
//    val r1 = lines.flatMap(_.split(" "))
//    val r2 = r1.map((_, 1))
//    r2.reduceByKey(_ + _).print()
//    ssc.start()
//    ssc.awaitTermination()
//
//
//  }
//
//}
