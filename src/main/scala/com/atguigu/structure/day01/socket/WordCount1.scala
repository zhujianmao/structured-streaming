package com.atguigu.structure.day01.socket

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object WordCount1 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("WordCount2").getOrCreate()

    val df: DataFrame = spark.readStream.format("socket").option("host", "hadoop102").option("port", 9999).load()

    val result: StreamingQuery = df.writeStream.format("console").outputMode("update").start()

    result.awaitTermination
    spark.close()
  }
}
