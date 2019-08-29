package day7_31

import java.lang

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}

object MeanSolver {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().appName("MeanSolver").master("local[2]").getOrCreate()
    val r1: Dataset[lang.Long] = spark.range(1, 11)
    //    r1.createTempView("v1")
    val m1: Meanget = new Meanget
    spark.udf.register("mean", m1)
    //    val rr: DataFrame = spark.sql("select mean(id) result from v1")
    //    rr.show()
    import spark.implicits._

    val result = r1.agg(m1($"id").as("geomean"))

    result.show()
  }

}

class Meanget extends UserDefinedAggregateFunction {
  override def inputSchema: StructType = StructType(List(StructField("value", DoubleType)))

  override def bufferSchema: StructType = StructType(List(StructField("counts", LongType),
    StructField("prod", DoubleType)))

  override def dataType: DataType = DoubleType

  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 1.0
  }

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getLong(0) + 1L
    buffer(1) = buffer.getDouble(1) * input.getDouble(1)
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getDouble(1) * buffer2.getDouble(1)
  }

  override def evaluate(buffer: Row): Any = {
    math.pow(buffer.getDouble(1), 1.toDouble / buffer.getLong(0))
  }
}
