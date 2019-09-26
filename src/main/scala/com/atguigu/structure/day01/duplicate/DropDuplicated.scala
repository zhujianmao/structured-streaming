package com.atguigu.structure.day01.duplicate

import java.sql.Timestamp

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger

/**
  * 1,2019-09-14 11:50:00,dog
  * 2,2019-09-14 11:51:00,dog
  * 1,2019-09-14 11:50:00,dog  id重复,不输出
  * 3,2019-09-14 11:53:00,dog
  * 1,2019-09-14 11:50:00,dog  id重复,不输出
  * 4,2019-09-14 11:45:00,dog  数据过期,不输出
  */
object DropDuplicated {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("DropDuplicated")
      .getOrCreate()
    import spark.implicits._

    spark.readStream
      .format("socket")
      .option("host", "hadoop102")
      .option("port", 9999)
      .load
      .as[String]
      .map(line => {
        val splits: Array[String] = line.split(",")
        (splits(0), Timestamp.valueOf(splits(1)), splits(2))
      })
      .toDF("id", "ts", "name")
      .withWatermark("ts", "2 minutes")
      .dropDuplicates("id")
      .writeStream
      .trigger(Trigger.ProcessingTime(1000))
      .format("console")
      .outputMode("append")
      .start
      .awaitTermination

  }
}
