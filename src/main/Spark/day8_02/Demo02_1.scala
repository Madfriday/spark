//package day8_02
//
//import org.apache.spark.{HashPartitioner, SparkConf}
//import org.apache.spark.streaming.kafka.KafkaUtils
//import org.apache.spark.streaming.{Seconds, StreamingContext}
//
//object Demo02_1 {
//  val f = (it: Iterator[(String, Seq[Int], Option[Int])]) => {
//    it.map(x => {
//      (x._1, x._2.sum + x._3.getOrElse(0))
//    })
//  }
//
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("Demo02_1").setMaster("local[2]")
//    val ssc = new StreamingContext(conf, Seconds(2))
//    ssc.checkpoint("./ck")
//    val zkQuorum = "node1:2181,slave1:2181,slave2:2181"
//    val groupId = "g100"
//    val topic = Map[String, Int]("you" -> 1)
//    val data = KafkaUtils.createStream(ssc, zkQuorum, groupId, topic)
//    val r1 = data.map(_._2).flatMap(_.split(" ")).map((_, 1))
//    val r2 = r1.updateStateByKey(f,
//      new HashPartitioner(ssc.sparkContext.defaultParallelism)
//      , true)
//    r2.print()
//    ssc.start()
//    ssc.awaitTermination()
//
//  }
//
//}
