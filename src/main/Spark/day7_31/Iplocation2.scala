package day7_31

import day7_28.Utils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

object Iplocation2 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("Iplocation2").master("local[2]").getOrCreate()
    import spark.implicits._
    val lines = spark.read.textFile("D:\\w.jar\\ip\\ip.txt")
    val bd = lines.map(x => {
      val cont = x.split("[|]")
      val fNum = cont(2).toLong
      val lNum = cont(3).toLong
      val province = cont(6)
      (fNum, lNum, province)
    })
    val r1 = bd.collect()
    val broad_r1: Broadcast[Array[(Long, Long, String)]] = spark.sparkContext.broadcast(r1)

    val ips: Dataset[String] = spark.read.textFile("D:\\w.jar\\ip\\access.log")
    val ip0: DataFrame = ips.map(x => {
      val pro = x.split("[|]")(1)
      val ipNum = Utils.ip2Long(pro)
      ipNum
    }).toDF("ipNum")
    ip0.createTempView("v1")
    spark.udf.register("ip2provice", (ipnum: Long) => {
      val get_v: Array[(Long, Long, String)] = broad_r1.value
      val fil: Int = Utils.binanrySearch(get_v, ipnum)
      var pro = "null"
      if (fil != -1) {
        pro = get_v(fil)._3
      }
      pro
    })
    val r2 = spark.sql("select ip2provice(ipNum) pro,count(*) as cb from v1 group by pro order by cb desc")
    r2.show()
  }

}