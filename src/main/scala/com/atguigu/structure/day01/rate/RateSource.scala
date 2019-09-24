package com.atguigu.structure.day01.rate

import org.apache.spark.sql.SparkSession

object RateSource {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("RateSource").getOrCreate()

    val df = spark.readStream
      .format("rate")
      .option("rowsPerSecond", 10)
      .option("rampUpTime", 1)
      .option("numPartitions", 2)
      .load

    df.writeStream
      .format("console")
      .outputMode("update")
      .option("truncate",false)
      .start
      .awaitTermination

    spark.close()

  }
}
