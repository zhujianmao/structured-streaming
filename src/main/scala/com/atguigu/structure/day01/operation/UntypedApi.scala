package com.atguigu.structure.day01.operation

import org.apache.spark.sql.SparkSession

object UntypedApi {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("UntypedApi").getOrCreate()
    import spark.implicits._

    val df = spark.readStream
      .format("socket")
      .option("host", "hadoop102")
      .option("port", 9999)
      .load

    val df2 = df.selectExpr("cast (value as string)")
      .as[String]
      .flatMap(_.split(" "))
      .toDF("word")
      .select("word")
      .groupBy("word")
      .count

    df2.writeStream.outputMode("update").format("console").start.awaitTermination
    spark.close()
  }
}
