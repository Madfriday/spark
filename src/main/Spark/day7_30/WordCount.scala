package day7_30

import org.apache.spark.sql.SparkSession

/**
 * wordcount built by sql2.x
 */
object WordCount {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("WordCount").master("local[2]").getOrCreate()

    val lines = spark.read.textFile("d:/w.jar/wordcount.txt")
    import spark.implicits._
    val words = lines.flatMap(_.split(" "))
    words.createTempView("v1")
    val r1 = spark.sql("select value,count(*) as rb from v1 group by value order by rb desc")
    r1.show()

  }
}
