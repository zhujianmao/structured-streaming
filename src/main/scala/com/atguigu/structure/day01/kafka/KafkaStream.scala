package com.atguigu.structure.day01.kafka

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger

/**
  * 流式处理读取Kafka
  */
object KafkaStream {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("KafkaBatch").getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
      .option("subscribe", "test")
      .load
      .selectExpr("cast( value as string)")
      .as[String]
      .flatMap(_.split(" "))
      .groupBy("value")
      .count()

    df.writeStream
      .trigger(Trigger.ProcessingTime(1000))
      .format("console")
      .outputMode("update")
      .start
      .awaitTermination()

    spark.close()

  }
}
