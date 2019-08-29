package day8_03

import day7_28.Utils
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD

object CUtils {
  def calculateincome(x: RDD[Array[String]]): Unit = {
    val p = x.map(y => {
      val price = y(4).toDouble
      price
    })
    val sum_p = p.reduce(_ + _)
    val jedis = Redis_demo.get_pool()
    jedis.incrByFloat("Amount", sum_p)
    jedis.close()
  }

  def calculateItem(x: RDD[Array[String]]) = {
    val r1 = x.map(y => {
      val item = y(2)
      val price = y(4).toDouble
      (item, price)
    })
    val r2 = r1.reduceByKey(_ + _)
    r2.foreachPartition(z => {
      val jedis = Redis_demo.get_pool()
      z.foreach(x1 => {
        jedis.incrByFloat(x1._1, x1._2)
      })
      jedis.close()
    })

  }

  def calculateZone(v: Broadcast[Array[(Long, Long, String)]], x: RDD[Array[String]]): Unit = {

    val r1 = x.map(y => {
      val had = v.value
      val ip = y(1)
      val price = y(4).toDouble
      val use_num = Utils.ip2Long(ip)
      val pan = Utils.binanrySearch(had, use_num)
      var province = "null"
      if (pan != -1) {
        province = had(pan)._3
      }
      (province, price)
    })
    val va = r1.reduceByKey(_ + _)
    va.foreachPartition(z => {
      val jedis = Redis_demo.get_pool()
      z.foreach(zz => {
        jedis.incrByFloat(zz._1, zz._2)
      })
      jedis.close()
    })
  }
}
