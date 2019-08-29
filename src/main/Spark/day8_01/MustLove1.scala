package day8_01

import org.apache.spark.sql.SparkSession

object MustLove1 {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("MustLove1")
      .master("local[2]").getOrCreate()
    import spark.implicits._
    val sc = spark.read.textFile("D:\\w.jar\\teathear")
    val df1 = sc.map(x => {
      val teacher = x.split("/")(3)
      val subject = x.split("/")(2).split("[.]")(0)
      ((subject, teacher), 1)
    }).toDF("subject", "teacher")
    df1.createTempView("v1")
    val r1 = spark.sql("select subject,teacher,count(1) rn from v1 group by subject,teacher")
    r1.createTempView("v2")
    val r2 = spark.sql("select * from (select subject,teacher,rn,row_number() over(partition by" +
      " subject order by rn desc) kk from v2) tm  where tm.kk <= 2")
    r2.show()

  }

}