package day7_28

import org.apache.spark.{SparkConf, SparkContext}

object IpLocation {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("IpLocation").setMaster("local[4]")
    val sc = new SparkContext(conf)
    val rules: Array[(Long, Long, String)] = Utils.readRelus(args(0))
    val broadrules = sc.broadcast(rules)
    val lines = sc.textFile(args(1))


    val func = (line: String) => {
      val get1 = line.split("[|]")(1)
      val res = Utils.ip2Long(get1)
      val relues_line = broadrules.value
      val key1 = Utils.binanrySearch(relues_line, res)
      var pr1 = "null"
      if (key1 != -1) {
        pr1 = relues_line(key1)._3
      }
      (pr1, 1)
    }

    val final_ = lines.map(func)
    val op = final_.reduceByKey(_ + _)
    println(op.collect().toBuffer)

  }

}
