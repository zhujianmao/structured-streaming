package com.atguigu.structure.day01.source.kafka

import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 批处理读取Kafka
  *
  *   得到的 df 的 schema 是固定的:
  *         key,value,topic,partition,offset,timestamp,timestampType
  * 这种模式一般需要设置消费的其实偏移量和结束偏移量,
  * 如果不设置 checkpoint 的情况下, 默认起始偏移量 earliest, 结束偏移量为 latest.
  * 该模式为一次性作业(批处理), 而非持续性的处理数据.
  */
object KafkaBatch {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .master("local[*]")
                                          .appName("KafkaBatch")
                                          .getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.read
                              .format("kafka")
                              .option("kafka.bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092")
                              .option("subscribe", "test")
                              //.option("startingOffsets","earliest")
                              //.option("endingOffsets","latest")
                              .option("startingOffsets","""{"test":{"0":200001}}""")
                              //指定topic在具体partition分区读取开始的offset偏移量
                              .option("endingOffsets","""{"test":{"0":200005}}""")
                                //指定topic在具体partition分区读取终止的offset偏移量
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
