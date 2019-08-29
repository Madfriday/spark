package day7_28

import java.sql.DriverManager

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object IpLocation2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpLocation2").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val relues_lines = sc.textFile("d:/w.jar/ip/ip.txt")
    val rules1: RDD[(Long, Long, String)] = relues_lines.map(x => {
      val a1 = x.split("[|]")
      val fistNum = a1(2).toLong
      val lastNum = a1(3).toLong
      val provice1 = a1(6)
      (fistNum, lastNum, provice1)
    })
    val rules2 = rules1.collect()
    val braod_relues = sc.broadcast(rules2)
    val lines = sc.textFile("d:/w.jar/ip/access.log")
    val final_op = lines.map(x => {
      val f = x.split("[|]")(1)
      val long1 = Utils.ip2Long(f)
      val r1 = braod_relues.value
      val ii = Utils.binanrySearch(r1, long1)
      var province = "null"
      if (ii != -1) {
        province = r1(ii)._3
      }
      (province, 1)
    })
    val red = final_op.reduceByKey(_ + _)
    println(red.collect().toBuffer)
    red.foreach(x => {
      val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=utf-8"
        , "root", "123456")
      val p = conn.prepareStatement("INSERT INTO big VALUES(?,?)")
      p.setString(1, x._1)
      p.setInt(2, x._2)
      p.executeUpdate()
      p.close()
      conn.close()
    })
    //red.foreachPartition(x => {
    //  val conn = DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8"
    //    ,"root","123456")
    //  val p = conn.prepareStatement("INSERT INTO big VALUES(?,?)")
    //  x.foreach(y =>{
    //    p.setString(1,y._1)
    //    p.setInt(2,y._2)
    //    p.executeUpdate()
    //
    //  })
    //  p.close()
    //  conn.close()
    //})
  }


}

