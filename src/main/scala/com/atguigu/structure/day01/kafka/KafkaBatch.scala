package com.atguigu.structure.day01.kafka

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 批处理读取Kafka
  */
object KafkaBatch {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("KafkaBatch").getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read.format("kafka")
      .option("kafka.bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
      .option("subscribe", "test")
      //.option("startingOffsets","earliest")
      //.option("endingOffsets","latest")
      .option("startingOffsets","""{"test":{"0":200001}}""")  //指定topic在具体partition分区读取开始的offset偏移量
      .option("endingOffsets","""{"test":{"0":200005}}""")  //指定topic在具体partition分区读取终止的offset偏移量
      .load
      .selectExpr("cast( value as string)")
      .as[String]
      .flatMap(_.split(" "))
      .groupBy("value")
      .count()

    df.write
      .format("console")
      .save

    spark.close()

  }
}
