package day8_13

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object Util {
  def calculateByTime(x : String): Unit = {
   val spark = SparkSession.builder().appName("Utils").master("local[2]").getOrCreate()
    //

  }

}
