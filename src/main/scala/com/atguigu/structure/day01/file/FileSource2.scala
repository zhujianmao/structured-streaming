package com.atguigu.structure.day01.file

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * 一个目录下有子目录,且目录格式一致
  *
  */
object FileSource2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder().master("local[*]").appName("FileSource2").getOrCreate()

    val scheme = StructType(StructField("name", StringType) :: StructField("age", IntegerType) :: StructField("sex", StringType) :: Nil)
    val df = spark.readStream
      .schema(scheme)
      .csv("E:\\input\\csvdir")

    df.writeStream.trigger(Trigger.ProcessingTime(1000))
      .format("console")
      .outputMode("update")
      .start
      .awaitTermination
    spark.close()
  }
}
