package day7_29

import org.apache.spark.{SparkConf, SparkContext}

object Custom2 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Custom2").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val uses = Array("l 3 9", "z 8 7", "v 3 4")
    val line = sc.parallelize(uses)
    val uff = line.map(x => {
      val orin = x.split(" ")
      val name = orin(0)
      val age = orin(1).toInt
      val face = orin(2).toInt
      (name, age, face)
    })
    //    import Compared.Comparing2
    //    val fin = uff.sortBy(x => Girl(x._1,x._2,x._3))
    //    println(fin.collect().toBuffer)

    //    val fin = uff.sortBy(x => (-x._3,x._2))
    //    println(fin.collect().toBuffer)
    implicit val comp = Ordering[(Int, Int)].on[(String, Int, Int)](x => (-x._3, x._2))
    val fin = uff.sortBy(x => x)
    println(fin.collect().toBuffer)
  }

  case class Girl(val Name: String, val age: Int, val face: Int) {}

}
