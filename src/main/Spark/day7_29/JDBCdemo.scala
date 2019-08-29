package day7_29

import java.sql.DriverManager

import org.apache.spark.rdd.JdbcRDD
import org.apache.spark.{SparkConf, SparkContext}

object JDBCdemo {
  val getConn = () => {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/bigdata?characterEncoding=UTF-8", "root", "123565")
}

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("JDBCdemo").setMaster("local[2]")
    val sc = new SparkContext(conf)
    val fin = new JdbcRDD(sc, getConn, "select * from User where id >= ? and id <= ?", 1, 5,
      2, rs => {
        val id = rs.getInt(1)
        val name = rs.getString(2)
        val age = rs.getInt(3)
        (id, name, age)
      })
    println(fin.collect().toBuffer)
  }

}
