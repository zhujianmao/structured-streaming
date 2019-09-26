package com.atguigu.structure.day01.outputsink

import java.util.{Timer, TimerTask}

import org.apache.spark.sql.streaming.StreamingQuery
import org.apache.spark.sql.{DataFrame, SparkSession}

object MemorySink {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[2]")
      .appName("MemorySink")
      .getOrCreate()
    import spark.implicits._

    val lines: DataFrame = spark.readStream
      .format("socket") // 设置数据源
      .option("host", "hadoop102")
      .option("port", 9999)
      .load

    val words: DataFrame = lines.as[String]
      .flatMap(_.split("\\W+"))
      .groupBy("value")
      .count()

    val query: StreamingQuery = words.writeStream
      .outputMode("complete")
      .format("memory") // memory sink
      .queryName("word_count") // 内存临时表名
     // .trigger(Trigger.ProcessingTime(3000))
      .start

    val timer: Timer = new Timer(true)
    val task: TimerTask = new TimerTask {
      override def run(): Unit = {
        spark.sql("select * from word_count order by count desc limit 2").show
      }
    }
    timer.scheduleAtFixedRate(task,0,3000)

    query.awaitTermination()


  }

}
