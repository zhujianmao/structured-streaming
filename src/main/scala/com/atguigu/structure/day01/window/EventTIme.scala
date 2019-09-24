package com.atguigu.structure.day01.window

import java.sql.Timestamp

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

object EventTIme {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("EventTIme").getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.readStream
      .format("socket")
      .option("host", "hadoop102")
      .option("includeTimestamp", value = true)
      .option("port", 7777)
      .load

    import org.apache.spark.sql.functions._
    val wordCount: DataFrame = df.as[(String, Timestamp)].flatMap {
      case (word, ts) => word.split(" ").map(w => (w, ts))
    }.toDF("word", "timeStamp")
      .groupBy(
        window($"timeStamp", "12 minutes", "4 minutes"),
        $"word"
      ).count
        .sort("window","word")

    wordCount.writeStream
      .format("console")
      .outputMode("complete")
      .trigger(Trigger.ProcessingTime(0))
      .option("truncate", value = false)
      .start
      .awaitTermination

    spark.close()
  }

}
