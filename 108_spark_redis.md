###redis

Redis是目前一个非常优秀的key-value存储系统。和Memcached类似，它支持存储的value类型相对更多，包括string(字符串)、list(链表)、set(集合)、zset(sorted set有序集合)和hash（哈希类型）。  
  
Redis3.x支持集群模式，更加可靠！

---
redis的安装:

1. 下载redis3的稳定版本，下载地址http://download.redis.io/releases/redis-3.2.11.tar.gz  
2. 上传redis-3.2.11.tar.gz到服务器  
3. 解压redis源码包  
tar -zxvf redis-3.2.11.tar.gz -C /usr/local/src/  
4. 进入到源码包中，编译并安装redis  
cd /usr/local/src/redis-3.2.11/  
make && make install  
5. 报错，缺少依赖的包  
缺少gcc依赖（c的编译器）  
6. 配置本地YUM源并安装redis依赖的rpm包  
yum -y install gcc  
7. 编译并安装 
make && make install  
8. 报错，原因是没有安装jemalloc内存分配器，可以安装jemalloc或直接输入
make MALLOC=libc && make install  
9. 重新编译安装   
make MALLOC=libc && make install  
10. 在所有机器的/usr/local/下创建一个redis目录，然后拷贝redis自带的配置文  件redis.conf到/usr/local/redis
mkdir /usr/local/redis  
cp /usr/local/src/redis-3.2.11/redis.conf /usr/local/redis  
11. 修改所有机器的配置文件redis.conf  
daemonize yes  #redis后台运行  
appendonly yes  #开启aof日志,它会每次写操作都记录一条日志  
bind 192.168.1.207  
12. 启动所有的redis节点  
cd /usr/local/redis  
**redis-server redis.conf**   
13. 查看redis进程状态  
ps -ef | grep redis  
14. 使用命令行客户的连接redis  
**redis-cli -p 6379**    
15. 关闭redis  
redis-cli shutdown  
17. 配置redis密码  
config set requirepass 123  

---

scala API

```scala
object JedisConnectionPool{

  val config = new JedisPoolConfig()
  //最大连接数,
  config.setMaxTotal(20)
  //最大空闲连接数,
  config.setMaxIdle(10)
  //当调用borrow Object方法时，是否进行有效性检查 -->
  config.setTestOnBorrow(true)
  //10000代表超时时间（10秒）
  val pool = new JedisPool(config, "192.168.1.210", 6379, 10000, "123568")

  def getConnection(): Jedis = {
    pool.getResource
  }

  def main(args: Array[String]) {


  val conn = JedisConnectionPool.getConnection()

  val r1 = conn.get("xiaoniu")

 conn.incrBy("xiaoniu", -50)

   val r2 = conn.get("xiaoniu")

  println(r2)

  val r = conn.keys("*")
   import scala.collection.JavaConversions._
  for(p <- r) {
    println(p + " : " + conn.get(p))
}

```

