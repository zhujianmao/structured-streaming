package com.atguigu.structure.day01.operation

import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

/**
  * {"name": "Michael","age": 29,"sex": "female"}
  * {"name": "Andy","age": 30,"sex": "male"}
  * {"name": "Justin","age": 19,"sex": "male"}
  * {"name": "Lisi","age": 18,"sex": "male"}
  * {"name": "zs","age": 10,"sex": "female"}
  * {"name": "zhiling","age": 40,"sex": "female"}
  *
  *  直接执行 sql
  */
object SqlApi {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
                                        .builder()
                                        .master("local[*]")
                                        .appName("SqlApi")
                                        .getOrCreate()

    val peopleSchema: StructType = new StructType()
                                    .add("name", StringType)
                                    .add("age", LongType)
                                    .add("sex", StringType)
    val peopleDF: DataFrame = spark.readStream
                                    .schema(peopleSchema)
                                    .json("E:/input/json")

    peopleDF.createOrReplaceTempView("people") //创建临时表

    val result: DataFrame = spark.sql("select name from people where age > 20")


                              result.writeStream
                              .outputMode("append")
                              .format("console")
                              .start
                              .awaitTermination()
  }
}

