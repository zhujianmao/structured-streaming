package com.atguigu.structure.day01.unsupport

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

object UnSupportOperation {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("UnSupportOperation")
      .getOrCreate()
    import spark.implicits._

    val inputDf = spark.readStream
      .format("socket")
      .option("host", "hadoop102")
      .option("port", 9999)
      .load
    inputDf.as[String].map(line => {
      val splits = line.split(" ")
      (splits(0), splits(1))
    }).toDF("name", "age").createOrReplaceTempView("user")

    val result: DataFrame = spark.sql("select distinct(name) from user")

    result.writeStream
          .format("console")
          .trigger(Trigger.ProcessingTime(1000))
          .outputMode("update")
          .start
          .awaitTermination

  }

}
