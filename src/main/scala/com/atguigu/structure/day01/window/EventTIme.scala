package com.atguigu.structure.day01.window

import java.sql.Timestamp

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *   读取数据的时候自动加上时间戳----数据的时间是延后于事件产生的时间
  *  option("includeTimestamp", value = true)  会自动加上时间戳
  */
object EventTIme {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .master("local[*]")
                                          .appName("EventTIme")
                                          .getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.readStream
                              .format("socket")
                              .option("host", "hadoop102")
                              .option("includeTimestamp", value = true) //会自动加上时间戳
                              .option("port", 9999)
                              .load

    import org.apache.spark.sql.functions._  //导入functions下的函数操作

    val wordCount: DataFrame = df.as[(String, Timestamp)]
                                .flatMap {
                                  case (word, ts) => word.split(" ").map(w => (w, ts))
                                }
                                .toDF("word", "timeStamp")
                                .groupBy(
                                  window($"timeStamp", "12 minutes", "4 minutes"),
                                  $"word"
                                )
                                .count

    wordCount.writeStream
            .format("console")
            .outputMode("update")
            .trigger(Trigger.ProcessingTime(1000))
            .option("truncate", value = false)
            .start
            .awaitTermination
  }

}
