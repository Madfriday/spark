package day7_31

import day7_28.Utils
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Iplocation {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Iplocation").master("local[2]").getOrCreate()
    import spark.implicits._
    val lines = spark.read.textFile("D:\\w.jar\\ip\\ip.txt")
    val bd = lines.map(x => {
      val cont = x.split("[|]")
      val fNum = cont(2).toLong
      val lNum = cont(3).toLong
      val province = cont(6)
      (fNum, lNum, province)
    }).toDF("fN", "lN", "pro")
    val ips: Dataset[String] = spark.read.textFile("D:\\w.jar\\ip\\access.log")
    val ip0: DataFrame = ips.map(x => {
      val pro = x.split("[|]")(1)
      val ipNum = Utils.ip2Long(pro)
      ipNum
    }).toDF("ipNum")
    bd.createTempView("v1")
    ip0.createTempView("v2")
    val r1 = spark.sql("SELECT pro,count(*)  cn FROM v2 JOIN v1 on (ipNum >= Fn AND ipNum <= LN) group by pro order by cn desc")
    r1.show()


  }
}
