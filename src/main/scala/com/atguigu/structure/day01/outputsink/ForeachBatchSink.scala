package com.atguigu.structure.day01.outputsink

import java.sql.{Connection, DriverManager, PreparedStatement}
import java.util.Properties

import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{ForeachWriter, SparkSession}

object ForeachBatchSink {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .master("local[2]")
                                          .appName("ForeachBatchSink")
                                          .getOrCreate()
    import spark.implicits._
    val props: Properties = new Properties()
    props.setProperty("user", "root")
    props.setProperty("password", "123456")

    spark.readStream
          .format("socket")
          .option("host", "hadoop102")
          .option("port", 9999)
          .load
          .as[String]
          .flatMap(_.split("\\W+").map(word=> Integer.valueOf(word)))
          .writeStream
          .outputMode("update")
          .foreachBatch((df, batchId) => {  // 当前分区id, 当前批次id
            if (df.count() != 0) {
             // df.cache()
              df.persist()
              df.write.json(s"./$batchId")
              df.write.mode("overwrite").jdbc("jdbc:mysql://hadoop104:3306/mytest", "user", props)
              df.unpersist()
            }
          }
          )
        .trigger(Trigger.ProcessingTime(1000))
          .start
          .awaitTermination
  }

}
