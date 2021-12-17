package com.suke_w.scala.sparksql

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object Rdd2DataFrameByProgrameScala {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .master("local")
      .appName("Rdd2DataFrameByProgrameScala")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val dataRDD = sc.parallelize(Array(("zhangsan", "swimming", 32), ("lisi", "running", 28)))

    import sparkSession.implicits._
    val rowRDD = dataRDD.map(row => Row(row._1, row._2, row._3))


    val schema = StructType(Array(
      StructField("name", StringType, true),
      StructField("game", StringType, true),
      StructField("age", IntegerType, true)
    ))

    val dataFrame = sparkSession.createDataFrame(rowRDD, schema)

    dataFrame.createOrReplaceTempView("student")

    val resultDF = sparkSession.sql("select * from student where age > 29")

    resultDF.show()

    val resultRDD = resultDF.rdd
    resultRDD.map(row => (row.getAs("name").toString,row.getAs("game").toString,row.getAs("age").toString.toInt))
      .collect()
      .foreach(println(_))



    sparkSession.stop()

  }
}
