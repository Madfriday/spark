package day7_27

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object Subject_teacher_04 {
  def main(args: Array[String]): Unit = {
    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("Subject_teacher_04").setMaster("local[4]")
      val cont = new SparkContext(conf)
      val sc = cont.textFile("D:\\w.jar\\teathear")
      val try_1 = sc.map(x => {
        val teacher = x.split("/")(3)
        val subject = x.split("/")(2).split("[.]")(0)
        ((subject, teacher), 1)
      })
      val subs = try_1.map(_._1._1).distinct().collect()
      val part1 = new SubjectP(subs)
      val t1 = try_1.reduceByKey(part1, _ + _)
      val tr = t1.mapPartitions(it => it.toList.sortBy(_._2).reverse.take(3).iterator)
      println(tr.collect().toBuffer)


    }
  }

  class SubjectP(sbs: Array[String]) extends Partitioner {
    val hsmap = new mutable.HashMap[String, Int]()
    var c = 0
    for (sb <- sbs) {
      hsmap(sb) = c
      c += 1
    }

    override def numPartitions: Int = sbs.length

    override def getPartition(key: Any): Int = {
      val subj = key.asInstanceOf[(String, String)]._1
      hsmap(subj)
    }
  }

}
