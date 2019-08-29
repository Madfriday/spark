package day7_27

import org.apache.spark.{Partitioner, SparkConf, SparkContext}

import scala.collection.mutable

object Subject_teacher_03 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Subject_teacher_03").setMaster("local[4]")
    val cont = new SparkContext(conf)
    val sc = cont.textFile("D:\\w.jar\\teathear")
    val try_1 = sc.map(x => {
      val teacher = x.split("/")(3)
      val subject = x.split("/")(2).split("[.]")(0)
      ((subject, teacher), 1)
    })
    //获得所有学科，且为array格式
    val subjects = try_1.map(_._1._1).distinct().collect()
    //自定义一个分区器，将相同学科分为同一区
    val partpa = new SubjectP(subjects)
    val p1 = try_1.partitionBy(partpa)
    //按不同的区排名即可
    val p2 = p1.mapPartitions(it => it.toList.sortBy(_._2).take(3).iterator)
    println(p2.collect().toBuffer)


  }

  class SubjectP(sbs: Array[String]) extends Partitioner {
    //用一个hashmap存储学科与对应的区
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
