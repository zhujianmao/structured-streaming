package com.atguigu.structure.day01.outputsink

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

object FileSink {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("FileSink")
      .getOrCreate()
    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket") // 设置数据源
      .option("host", "hadoop102")
      .option("port", 9999)
      .load

    val words: DataFrame = lines.as[String].flatMap(line => {
      line.split("\\W+").map(word => {
        (word, word.reverse)
      })
    }).toDF("原单词", "反转单词")

    words.writeStream
      .outputMode("append")  //仅支持append模式
      .format("json") //  // 支持 "orc", "json", "csv"
      .option("path", "./filesink") // 输出目录
      .option("checkpointLocation", "./ck1") // 必须指定 checkpoint 目录
      .trigger(Trigger.ProcessingTime(1000))
      .start
      .awaitTermination()
  }
}
