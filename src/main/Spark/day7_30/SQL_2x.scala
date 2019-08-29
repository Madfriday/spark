package day7_30

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SparkSession}

object Sql_2x {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Sql_2x").master("local[2]").getOrCreate()
    import spark.implicits._
    val lines = spark.sparkContext.textFile("d:/w.jar/p.txt")
    val values = lines.map(x => {
      val line = x.split(",")
      val id = line(0).toLong
      val name = line(1)
      val age = line(2).toInt
      val fv = line(3).toDouble
      Row(id, name, age, fv)
    })
    val stu1 = StructType(List(StructField("id", LongType, true),
      StructField("name", StringType, true),
      StructField("age", IntegerType, true),
      StructField("fv", DoubleType, true)))

    val bdf = spark.createDataFrame(values, stu1)
    val r1 = bdf.where($"fv" > 80).sort($"fv" desc, $"age" asc)
    r1.show()
  }

}
