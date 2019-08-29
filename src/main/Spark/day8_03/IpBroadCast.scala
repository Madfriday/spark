package day8_03

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.StreamingContext

object IpBroadCast {
  def BroadCast_Ip(ssc: StreamingContext, ipPath: String): Broadcast[Array[(Long, Long, String)]] = {
    val sc = ssc.sparkContext
    val lines = sc.textFile(ipPath)
    val line = lines.map(x => {
      val con = x.split("[|]")
      val fn = con(2).toLong
      val ln = con(3).toLong
      val province = con(6)
      (fn, ln, province)
    })
    val c1: Broadcast[Array[(Long, Long, String)]] = sc.broadcast(line.collect())
    c1
  }
}
