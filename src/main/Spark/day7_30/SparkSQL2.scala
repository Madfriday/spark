//package day7_30
//
//import org.apache.spark.{SparkConf, SparkContext}
//import org.apache.spark.sql.SQLContext
//
///**
// * a spark sql 1.x demo is built
// */
//object SparkSQL1 {
//  def main(args: Array[String]): Unit = {
//    val conf = new SparkConf().setAppName("SparkSQL1").setMaster("local[2]")
//    val sc = new SparkContext(conf)
//    val qc = new SQLContext(sc)
//    val lines = sc.textFile("d:/w.jar/p.txt")
//    val values = lines.map(x => {
//      val line = x.split(",")
//      val id = line(0).toLong
//      val name = line(1)
//      val age = line(2).toInt
//      val fv = line(3).toDouble
//      Person(id, name, age, fv)
//    })
//    import qc.implicits._
//    val bdf = values.toDF()
//
//    //注册临时表
//    bdf.registerTempTable("t1")
//    val r1 = qc.sql("select * from t1 order By fv desc,age asc")
//    r1.show()
//    sc.stop()
//
//
//  }
//
//  case class Person(id: Long, name: String, age: Int, fv: Double) {}
//
//}
