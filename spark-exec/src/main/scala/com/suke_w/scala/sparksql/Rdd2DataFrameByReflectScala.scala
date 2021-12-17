package com.suke_w.scala.sparksql

import org.apache.spark.sql.SparkSession

object Rdd2DataFrameByReflectScala {
  def main(args: Array[String]): Unit = {

    val sparkSession = SparkSession.builder()
      .appName("Rdd2DataFrameByReflectScala")
      .master("local")
      .getOrCreate()

    val sc = sparkSession.sparkContext
    val dataRDD = sc.parallelize(Array(("javak", 18), ("tom", 24)))

    import sparkSession.implicits._
    val studf = dataRDD.map(tup => Student(tup._1, tup._2)).toDF

    studf.createOrReplaceTempView("student");

    val resultDF = sparkSession.sql("select * from student where age > 20")
    resultDF.show()

    val resultRDD = resultDF.rdd
   // resultRDD.map(row => Student(row(0).toString,row(1).toString.toInt))
   //   .collect()
   //   .foreach(println(_))

   resultRDD.map(row => Student(row.getAs("name"),row.getAs("age")))
     .collect()
     .foreach(println(_))


    sparkSession.stop()


  }
}

case class Student(name: String,age: Int)
