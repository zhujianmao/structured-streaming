package com.atguigu.structure.day01.outputsink

import java.sql.{Connection, DriverManager, PreparedStatement}

import org.apache.spark.sql.{ForeachWriter, SparkSession}

object ForeachSink {
  def main(args: Array[String]): Unit = {
    val spark: SparkSession = SparkSession.builder()
                                          .master("local[*]")
                                          .appName("ForeachBatchSink")
                                          .getOrCreate()
    import spark.implicits._

    spark.readStream
          .format("socket")
          .option("host", "hadoop102")
          .option("port", 9999)
          .load
          .as[String]
          .flatMap(_.split("\\W+"))
          .writeStream
          .outputMode("append")
          .foreach(new ForeachWriter[String] {
            var conn: Connection = _
            val sql: String = "insert into user values(?) on duplicate key update id = ?"

            override def open(partitionId: Long, epochId: Long): Boolean = {
                println("获取连接")
                Class.forName("com.mysql.jdbc.Driver")
                conn = DriverManager.getConnection("jdbc:mysql://hadoop104:3306/mytest", "root", "123456")
                conn != null && !conn.isClosed
            }

            override def process(value: String): Unit = {
                println("开始写入数据")
                val statement: PreparedStatement = conn.prepareStatement(sql)
                statement.setInt(1, Integer.valueOf(value))
                statement.setInt(2, Integer.valueOf(value) * 100)
                statement.execute

                statement.close()
            }

            override def close(errorOrNull: Throwable): Unit = {
                println("关闭连接")
                conn.close()
            }
          })
          .start
          .awaitTermination

  }

}
