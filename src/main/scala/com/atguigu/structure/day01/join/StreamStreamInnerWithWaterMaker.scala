package com.atguigu.structure.day01.join

import java.sql.Timestamp

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *   对 2 个流式数据进行 join 操作. 输出模式仅支持append模式
  *
  * 第 1 个数据格式: 姓名,年龄,事件时间
  * lisi,female,2019-09-16 11:50:00
  * zs,male,2019-09-16 11:51:00
  * ww,female,2019-09-16 11:52:00
  * zhiling,female,2019-09-16 11:53:00
  * fengjie,female,2019-09-16 11:54:00
  * yifei,female,2019-09-16 11:55:00
  *
  * 第 2 个数据格式: 姓名,性别,事件时间
  * lisi,18,2019-09-16 11:50:00
  * zs,19,2019-09-16 11:51:00
  * ww,20,2019-09-16 11:52:00
  * zhiling,22,2019-09-16 11:53:00
  * yifei,30,2019-09-16 11:54:00
  * fengjie,98,2019-09-16 11:55:00
  */
object StreamStreamInnerWithWaterMaker {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("StreamStreamLeftoutWithWaterMaker")
      .getOrCreate()
    import spark.implicits._

    // 第 1 个 stream
    val nameSexStream: DataFrame = spark.readStream
      .format("socket")
      .option("host", "hadoop102")
      .option("port", 9999)
      .load
      .as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0), arr(1), Timestamp.valueOf(arr(2)))
      }).toDF("name", "sex", "ts1")
      .withWatermark("ts1", "2 minutes")

    // 第 2 个 stream
    val nameAgeStream: DataFrame = spark.readStream
      .format("socket")
      .option("host", "hadoop102")
      .option("port", 10000)
      .load
      .as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0), arr(1).toInt, Timestamp.valueOf(arr(2)))
      }).toDF("name", "age", "ts2")
      .withWatermark("ts2", "2 minutes")


    // join 操作
    val joinResult: DataFrame = nameSexStream.join(nameAgeStream, "name")

    joinResult.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime(1000))
      .start()
      .awaitTermination()
  }
}
