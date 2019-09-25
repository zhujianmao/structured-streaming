package com.atguigu.structure.day01.source.file

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

/**
  * 一个目录下有子目录,且目录格式一致
  *
  * 注意:
  *     当文件夹被命名为 “key=value” 形式时,
  *     Structured Streaming 会自动递归遍历当前文件夹下的所有子文件夹,
  *     并根据文件名实现自动分区.
  *   如果文件夹的命名规则不是“key=value”形式,
  *   则不会触发自动分区.
  *   另外, 同级目录下的文件夹的命名规则必须一致.
  *     lisi,male,18
  *     zhiling,female,28
  */
object FileSource2 {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
                                      .builder()
                                      .master("local[*]")
                                      .appName("FileSource2")
                                      .getOrCreate()

    val scheme: StructType = StructType(StructField("name", StringType)
                                          :: StructField("age", IntegerType)
                                          :: StructField("sex", StringType)
                                          :: Nil)
    val df: DataFrame = spark.readStream
                              .schema(scheme)
                              .csv("E:\\input\\csvdir") //不能子目录下文件格式不一致

    df.writeStream
      .trigger(Trigger.ProcessingTime(1000))
      .format("console")
      .outputMode("update")
      .start
      .awaitTermination
    spark.close()
  }
}
