package com.atguigu.structure.day01.operation

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  *  弱操作就是对DataFrame的操作
  */
object UntypedApi {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .master("local[*]")
                                          .appName("UntypedApi")
                                          .getOrCreate()
    import spark.implicits._

    val df: DataFrame = spark.readStream
                              .format("socket")
                              .option("host", "hadoop102")
                              .option("port", 9999)  //单词以空格分割
                              .load

    val df2: DataFrame = df.selectExpr("cast (value as string)")
                        .as[String]
                        .flatMap(_.split(" "))
                        .toDF("word")
                        .select("word")
                        .groupBy("word")
                        .count

                    df2.writeStream
                          .outputMode("update")
                          .trigger(Trigger.ProcessingTime(1000))
                          .format("console")
                          .start.awaitTermination
    spark.close()
  }
}
