package com.atguigu.structure.day01.operation

import org.apache.spark.sql.types.{LongType, StringType, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

/**
  * {"name": "Michael","age": 29,"sex": "female"}
  * {"name": "Andy","age": 30,"sex": "male"}
  * {"name": "Justin","age": 19,"sex": "male"}
  * {"name": "Lisi","age": 18,"sex": "male"}
  * {"name": "zs","age": 10,"sex": "female"}
  * {"name": "zhiling","age": 40,"sex": "female"}
  *
  * 强类型操作即是操作DataSet
  */
object TypeApi {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession
                                        .builder()
                                        .master("local[*]")
                                        .appName("SqlApi")
                                        .getOrCreate()
    import spark.implicits._

    val peopleSchema: StructType = new StructType()
                                    .add("name", StringType)
                                    .add("age", LongType)
                                    .add("sex", StringType)
    val peopleDF: DataFrame = spark.readStream
                                    .schema(peopleSchema)
                                    .json("E:/input/json")

    val peopleDS: Dataset[People] = peopleDF.as[People] // 转成 ds


    val df: Dataset[String] = peopleDS.filter(_.age > 20).map(_.name)

                            df.writeStream
                              .outputMode("append")
                              .format("console")
                              .start
                              .awaitTermination()
  }
}
case class People(name: String, age: Long, sex: String)