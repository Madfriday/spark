package day7_30

import day7_30.SparkSQL1.Person
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types._

object SparkSQL3 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("SparkSQL3").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val qc = new SQLContext(sc)
    val lines = sc.textFile("d:/w.jar/p.txt")
    import qc.implicits._
    val values = lines.map(x => {
      val line = x.split(",")
      val id = line(0).toLong
      val name = line(1)
      val age = line(2).toInt
      val fv = line(3).toDouble
      Row(id, name, age, fv)
    })

    val schema = StructType(List(StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)))
    val bdf = qc.createDataFrame(values, schema)
    val r1 = bdf.select("name", "age", "fv")
    val r2 = r1.orderBy($"fv" desc, $"age" asc)
    r2.show()
  }
}