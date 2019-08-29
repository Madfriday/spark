package day7_29

import org.apache.spark.{SparkConf, SparkContext}

object Custom {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("Custom").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val uses = Array("l 3 9", "z 8 7", "v 3 4")
    val line = sc.parallelize(uses)
    val uff = line.map(x => {
      val orin = x.split(" ")
      val name = orin(0)
      val age = orin(1).toInt
      val face = orin(2).toInt
      new User(name, age, face)
    })
    val fi = uff.sortBy(x => x)
    println(fi.collect().toBuffer)
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
      s"$name and age is $age and face is $face"
    }
  }

}
