package com.atguigu.structure.day01.inputsource.rate

import org.apache.spark.sql.{DataFrame, SparkSession}

object RateSource {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .master("local[*]")
                                          .appName("RateSource")
                                          .getOrCreate()

    val df: DataFrame = spark.readStream
                              .format("rate")
                              .option("rowsPerSecond", 1000000)  // 设置每秒产生的数据的条数, 默认是 1
                              .option("rampUpTime", 1)      // 设置多少秒到达指定速率 默认为 0
                              .option("numPartitions", 2)   // 设置分区数  默认是 spark 的默认并行度
                              .load

                        df.writeStream
                          .format("console")
                          .outputMode("update")
                          .option("truncate",value = false)  //是否显示齐全信息
                          .start
                          .awaitTermination

    spark.close()

  }
}
