package com.atguigu.structure.day01.outputsink

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

object KafkaSink {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("KafkaSink")
      .getOrCreate()
    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket") // 设置数据源
      .option("host", "hadoop102")
      .option("port", 9999)
      .load

    val words = lines.as[String]
      .flatMap(_.split("\\W+"))
      .groupBy("value")
      .count()
      .map(row => row.getString(0) + "," + row.getLong(1))
      .toDF("value")  // 写入数据时候, 必须有一列 "value"

    words.writeStream
      .outputMode("update")
      .format("kafka")
      .trigger(Trigger.ProcessingTime(1000))
      .option("kafka.bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092") // kafka 配置
      .option("topic", "test") // kafka 主题 主题未创建时会自动创建
      .option("checkpointLocation", "./ck2")  // 必须指定 checkpoint 目录
      .start
      .awaitTermination()


  }
}
