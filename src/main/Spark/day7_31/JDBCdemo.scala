package day7_31

import java.util.Properties

import org.apache.spark.sql.SparkSession

object JDBCDemo {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("JDBCDemo").master("local[2]").getOrCreate()
    import spark.implicits._
    val log = spark.read.format("jdbc").options(Map("url" -> "jdbc:mysql://localhost:3306/bigdata",
      "driver" -> "com.mysql.jdbc.Driver",
      "dbtable" -> "user",
      "user" -> "root",
      "password" -> "123456")
    ).load()
    val r1 = log.filter(r => {
      r.getAs[Int]("age") <= 15
    })
    val r2 = log.select($"id", $"name", $"age" * 10 as "new_age")
    r2.show()
    val prop = new Properties()
    prop.put("user", "root")
    prop.put("password", "123456")
    r2.write.mode("ignore").jdbc("jdbc:mysql://localhost:3306/bigdata", "new_1", prop)


  }

}

