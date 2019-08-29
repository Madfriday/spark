package day8_13

import java.util

import redis.clients.jedis.{JedisCluster, _}

import scala.collection.mutable

/**
 * Created by zx on 2017/10/30.
 */


object JedisConnectPool {

  private val config: JedisPoolConfig = new JedisPoolConfig()

  //最大连接数
  config.setMaxTotal(20)
  //最大空闲连接数
  config.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  config.setTestOnBorrow(true)

  //private val pool: JedisPool = new JedisPool(config, "192.168.100.101", 6379, 10000, "")

  val jedisClusterNodes = new util.HashSet[HostAndPort]()
  //Jedis Cluster will attempt to discover cluster nodes automatically
  jedisClusterNodes.add(new HostAndPort("192.168.100.101", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.100.102", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.100.103", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.100.104", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.100.105", 6379))
  jedisClusterNodes.add(new HostAndPort("192.168.100.106", 6379))
  val jedisCluster = new JedisCluster(jedisClusterNodes)


  def main(args: Array[String]): Unit = {
    val str = jedisCluster.get("abc123")

    println(str)


  }



}