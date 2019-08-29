package day7_31

import org.apache.commons.pool2.impl.GenericObjectPoolConfig
import redis.clients.jedis.JedisPool

object HH {
  def main(args: Array[String]): Unit = {
    val jedis = new JedisPool(new GenericObjectPoolConfig(),"192.168.9.12",6379,3000)
    val kk = jedis.getResource
    val r1 = kk.incrBy("bb",100)
    println(r1)
    val r2 = kk.keys("*")
    import scala.collection.JavaConversions._
    for (p <- r2) {
      println(p + ":" + kk.get(p))
    }
    kk.close()
  }

}
