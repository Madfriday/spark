package day7_27

import org.apache.spark.{SparkConf, SparkContext}

object Techear_01 {
  def main(args: Array[String]): Unit = {
    //    val conf = new SparkConf().setAppName("Techear_01").setMaster("local[4]")
    //    val sc = new SparkContext(conf)
    val l = "http://bigdata.edu360.cn/laozhang"
    val so = l.split("/")(2)
    val so1 = so.split("[.]")
    println(so1(0))
    //    val lines = sc.textFile("D:\\w.jar\\teathear")
    //    val line = lines.map(x => {
    //      val teacher = x.split("/")(3)
    ////      val subject = x.split("/")(2).split(".")(1)
    //      (teacher,1)
    //    })
    //    val res = line.reduceByKey(_+_)
    //    val f_res = res.sortBy(_._2,false)
    //    println(f_res.collect().toBuffer)

  }

}