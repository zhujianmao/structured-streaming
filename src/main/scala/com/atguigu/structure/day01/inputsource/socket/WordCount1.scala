package com.atguigu.structure.day01.inputsource.socket

import org.apache.spark.sql.{DataFrame, SparkSession}

object WordCount1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("WordCount1").getOrCreate()

    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "hadoop102")
      .option("port", 9999)
      .load()

    df.writeStream
      .format("console")
      .outputMode("update")
      .start()
      .awaitTermination

    spark.close()
  }
}
