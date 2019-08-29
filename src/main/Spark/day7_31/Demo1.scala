package day7_31

import org.apache.spark.sql.SparkSession

object Demo1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Demo1").master("local[2]").getOrCreate()
    import spark.implicits._
    val lines = spark.createDataset(List("1 china sichuan", "2 America florada"))
    val line = lines.map(x => {
      val sp = x.split(" ")
      val id = sp(0).toLong
      val name = sp(1)
      val nation = sp(2)
      (id, name, nation)
    })
    val bdf1 = line.toDF("id", "name11", "nation2")
    val lines1 = spark.createDataset(List("1 France paris", "3 england ludon"))
    val line2 = lines1.map(x => {
      val sp = x.split(" ")
      val id = sp(0).toLong
      val name = sp(1)
      val nation = sp(2)
      (id, name, nation)
    })
    val bdf2 = line2.toDF("id1", "name22", "nation1")
    //    bdf1.createTempView("v1")
    //    bdf2.createTempView("v2")
    //    val r1 = spark.sql("select name11,nation2 from v1 join v2 on v1.id = v2.id")
    //    r1.show()
    val r1 = bdf1.join(bdf2, $"id" === $"id1")
    r1.show()

  }
}