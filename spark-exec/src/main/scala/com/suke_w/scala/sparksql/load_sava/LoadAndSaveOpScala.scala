package com.suke_w.scala.sparksql.load_sava

import org.apache.spark.sql.{SaveMode, SparkSession}

object LoadAndSaveOpScala {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("LoadAndSaveOpScala")
      .getOrCreate()

    val stuDF = sparkSession.read
      .format("json")
      .load("D:\\Users\\suke_w\\Desktop\\git_repositoy\\123\\bigdata-exec\\spark-exec\\src\\main\\resources\\testFile\\student.text")
    val resultDF = stuDF.select("name", "age")
    resultDF.write
      .format("csv")
      .mode(SaveMode.Append)
      .save("D:\\Users\\suke_w\\Desktop\\git_repositoy\\123\\bigdata-exec\\spark-exec\\src\\main\\resources\\testFile\\student_csv")

    sparkSession.stop()
  }

}
