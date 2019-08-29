package day7_29

import day7_29.Custom.User
import org.apache.spark.{SparkConf, SparkContext}

object Custom1 {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Custom1").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val uses = Array("l 3 9", "z 8 7", "v 3 4")
    val line = sc.parallelize(uses)
    val uff = line.map(x => {
      val orin = x.split(" ")
      val name = orin(0)
      val age = orin(1).toInt
      val face = orin(2).toInt
      (name, age, face)
    })
    //    val fin = uff.sortBy(x => {
    //      new User(x._1,x._2,x._3)
    //    })
    val fin = uff.sortBy(x => Boy(x._1, x._2, x._3))
    println(fin.collect().toBuffer)

  }

  class User(val name: String, val age: Int, val face: Int) extends Ordered[User] with Serializable {
    override def compare(that: User): Int = {
      if (this.face == that.face) {
        this.age - that.age
      }
      else {
        that.face - this.face
      }
    }

    override def toString: String = {
      s"name :$name age: $age face: $face"
    }
  }

  case class Boy(val name: String, val age: Int, val face: Int) extends Ordered[Boy] {
    override def compare(that: Boy): Int = {
      if (this.face == that.face) {
        this.age - that.age
      }
      else {
        that.face - this.face
      }

    }

    override def toString: String = {
      s"name :$name age: $age face: $face"
    }
  }

}

