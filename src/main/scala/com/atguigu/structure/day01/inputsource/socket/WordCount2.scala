package com.atguigu.structure.day01.inputsource.socket

import org.apache.spark.sql.streaming.{StreamingQuery, Trigger}
import org.apache.spark.sql.{DataFrame, SparkSession}

object WordCount2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("WordCount2").getOrCreate()
    val df: DataFrame = spark.readStream
                              .format("socket")
                              .option("host", "hadoop102")
                              .option("port", 9999)
                              .load()
    import spark.implicits._
    val wordCount = df.as[String]
                                        .flatMap(_.split(" "))
                                        .groupBy("value")
                                        .count()
    //    df.as[String].flatMap(_.split(" ")).createOrReplaceTempView("word")
    //    val wordCount = spark.sql(
    //      """
    //        |select value,
    //        |count(value) count
    //        |from word
    //        |group by value
    //      """.stripMargin)
    val result: StreamingQuery = wordCount.writeStream
                                          .format("console")
                                          .trigger(Trigger.ProcessingTime(1000))
                                          .outputMode("complete")
                                          .start()

    result.awaitTermination
    spark.close()
  }
}
