package day8_13

import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.util.parsing.json.JSONObject

object Hdfsread {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("H1").master("local[2]").getOrCreate()
    //
    import spark.implicits._
    //

    val lines: DataFrame = spark.read.json("hdfs://192.168.9.11:9000/warn//20190814")
    //
   val re = lines.select("bikeNo").count()
    println(re)
//    //
//    line.createTempView("v1")
//    //
//    spark.sql("select bike,count(1) from v1 group by bike,types").show()

  }

}
