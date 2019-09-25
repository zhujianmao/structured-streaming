package com.atguigu.structure.day01.source.file

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * 一个目录下单文件,没有子目录
  *
  * lisi,male,18
  * zhiling,female,28
  */
object FileSource {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .master("local[*]")
                                          .appName("FileSource2")
                                          .getOrCreate()

    val scheme: StructType = StructType(StructField("name", StringType)
                                            :: StructField("age", IntegerType)
                                            :: StructField("sex", StringType)
                                            :: Nil)
    val df: DataFrame = spark.readStream
                              .format("csv")
                              .schema(scheme)
                              .load("E:\\input\\csv")

    df.writeStream
      .trigger(Trigger.ProcessingTime(1000))
      .format("console")
      .outputMode("update")
      .start
      .awaitTermination
    spark.close()
  }
}
