package day7_27

import org.apache.spark.{SparkConf, SparkContext}

object Subject_teacher {
  def main(args: Array[String]): Unit = {
    //g构建spark
    val conf = new SparkConf().setAppName("Subject_teacher").setMaster("local[4]")
    val cont = new SparkContext(conf)
    //读取文件内容
    val sc = cont.textFile("D:\\w.jar\\teathear")
    val lines = sc.map(x => {
      val teacher = x.split("/")(3)
      val subject = x.split("/")(2).split("[.]")(0)
      //将相应字段与1组成元组
      ((subject, teacher), 1)
    })
    //通过reducebykey计算相同key的次数
    val re1 = lines.reduceByKey(_ + _)
    //利用groupby将key的第一个值即学科作为map的key聚集所有相同key的数据
    val re2 = re1.groupBy(_._1._1)
    //利用mapvalues去除map中的value，因为key值相同，所以直接排序取得排名前三的
    val r3 = re2.mapValues(_.toList.sortBy(_._2).reverse.take(3))
    println(r3.collect().toBuffer)
  }
}
