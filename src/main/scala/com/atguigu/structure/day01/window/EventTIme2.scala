package com.atguigu.structure.day01.window

import java.sql.Timestamp

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *  event-time取数据里面的时间
  *  数据的格式:
  *       2019-09-25 10:55:00,a b c
  *
  *
  *   org.apache.spark.sql.catalyst.analysis.TimeWindowing
  *
  *  The windows are calculated as below:
  * maxNumOverlapping <- ceil(windowDuration / slideDuration)
  * for (i <- 0 until maxNumOverlapping)
  *    windowId <- ceil((timestamp - startTime) / slideDuration)
  *   windowStart <- windowId * slideDuration + (i - maxNumOverlapping) * slideDuration + startTime
  *   windowEnd <- windowStart + windowDuration
  *    return windowStart, windowEnd
  */
object EventTIme2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .master("local[*]")
                                          .appName("EventTIme2")
                                          .getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.readStream
                              .format("socket")
                              .option("host", "hadoop102")
                              .option("port", 9999)
                              .load

    import org.apache.spark.sql.functions._

    val wordCount: DataFrame = df.as[String]
                                  .flatMap(line =>{
                                      val split: Array[String] = line.split(",")
                                      split(1).split(" ").map((_, Timestamp.valueOf(split(0))))
                                  }).toDF("word", "timeStamp")
                                  .groupBy(
                                    window($"timeStamp", "10 minutes", "4 minutes"),
                                    $"word"
                                  ).count
                                    .sort("window","word")

                      wordCount.writeStream
                                .format("console")
                                .outputMode("complete")
                                .trigger(Trigger.ProcessingTime(1000))
                                .option("truncate", value = false)
                                .start
                                .awaitTermination
  }

}
