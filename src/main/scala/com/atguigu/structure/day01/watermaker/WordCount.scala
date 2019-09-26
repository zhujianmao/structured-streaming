package com.atguigu.structure.day01.watermaker

import java.sql.Timestamp

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *  watermaker水印下,
  *     append 输出过期的数据
  *     update 输出变化的数据
  *   水印过期是删除窗口内的数据,而不是说数据过期,对数据不进行处理,直接丢弃
  *
  */
object WordCount {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .master("local[*]")
                                          .appName("WordCount")
                                          .getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.readStream
                              .format("socket")
                              .option("host", "hadoop102")
                              .option("port", 9999)
                              .load

    import org.apache.spark.sql.functions._

    val wordCount: DataFrame = df.as[String]
                                .flatMap(line => {
                                  val split: Array[String] = line.split(",")
                                  split(1).split(" ").map((_, Timestamp.valueOf(split(0))))
                              })
                                .toDF("word", "timeStamp")
                                .withWatermark("timeStamp", "2 minutes")  //水印
                                .groupBy(
                                  window($"timeStamp", "10 minutes", "2 minutes"),
                                  $"word"
                                )
                                .count

                      wordCount.writeStream
                                .format("console")
                               // .outputMode("update")
                                .outputMode("append")
                                .trigger(Trigger.ProcessingTime(1000))
                                .option("truncate", value = false)
                                .start
                                .awaitTermination
  }

}
