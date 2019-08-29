package day8_03

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object Redis_demo {
  val conf = new JedisPoolConfig
  conf.setMaxTotal(20)
  conf.setMaxIdle(10)
  conf.setTestOnBorrow(true)
  val pool = new JedisPool(conf, "192.168.9.12", 6379, 10000)

  def get_pool(): Jedis = {
    pool.getResource
  }

  def main(args: Array[String]): Unit = {
    val read = Redis_demo.get_pool()

val r1 = read.incrBy("bb",100)
 println(r1)
    val r2 = read.keys("*")
    import scala.collection.JavaConversions._
    for (p <- r2) {
      println(p + ":" + read.get(p))
    }
    read.close()

  }

}
