package day7_27

import org.apache.spark.{SparkConf, SparkContext}

object Subject_teacher_02 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Subject_teacher_02").setMaster("local[4]")
    val cont = new SparkContext(conf)
    val sc = cont.textFile("D:\\w.jar\\teathear")
    //创建一个数组存放所有学科
    var arr = Array("bigdata","javaee","php")
    val try_1 = sc.map(x => {
      val teacher = x.split("/")(3)
      val subject = x.split("/")(2).split("[.]")(0)
      ((subject, teacher), 1)
    })
    val t1 = try_1.reduceByKey(_ + _)
    //利用过滤，拿到唯一学科，在此学科下直接排名即可
    for(a <- arr) {
      val filt = t1.filter(_._1._1 == a)
      val re = filt.sortBy(_._2, false).take(3)
    }

  }
}