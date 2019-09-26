package com.atguigu.structure.day01.join

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * 流式数据:
  * lisi,20
  * zhiling,40
  * ww,30
  */
object StreamStaticInner {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
      .builder()
      .master("local[*]")
      .appName("StreamStaticLeftOut")
      .getOrCreate()
    import spark.implicits._
    // 1. 静态 df
    val arr: Array[(String, String)] = Array(("lisi", "male"), ("zhiling", "female"), ("zs", "male"))
    val staticDF: DataFrame = spark.sparkContext.parallelize(arr).toDF("name", "sex")

    // 2. 流式 df
    val lines: DataFrame = spark.readStream
      .format("socket")
      .option("host", "hadoop102")
      .option("port", 9999)
      .load()
    val streamDF: DataFrame = lines.as[String]
      .map(line => {
        val arr: Array[String] = line.split(",")
        (arr(0), arr(1).toInt)
      })
      .toDF("name", "age")

    // 3. join   等值内连接  a.name=b.name
    val joinResult: DataFrame = streamDF.join(staticDF, "name")

    // 4. 输出
    joinResult.writeStream
      .outputMode("append")
      .format("console")
      .trigger(Trigger.ProcessingTime(1000))
      .start
      .awaitTermination()

  }
}
