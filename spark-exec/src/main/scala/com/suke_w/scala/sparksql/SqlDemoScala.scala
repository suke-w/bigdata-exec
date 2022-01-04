package com.suke_w.scala.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SqlDemoScala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
    conf.setMaster("local")

    val sparkSession = SparkSession.builder()
      .appName("SqlDemoScala")
      .config(conf)
      .getOrCreate()

    val frame_student = sparkSession.read.json("D:\\student.json")

    /**
     * DataFrame的算子操作
     */
    import sparkSession.implicits._
    //frame_student.printSchema()
    //frame_student.select("name","sex").show()
    //frame_student.select($"name",$"age" + 1).show()
    //frame_student.select($"name",$"age").show()
    //frame_student.show()
    //frame_student.filter($"age" > 20).show()
    //frame_student.where($"age" > 18).show()
    //frame_student.groupBy("age").count().show()
    //frame_student.groupBy("name").count().show()

    /**
     * DataFrame的sql操作
     */
    frame_student.createOrReplaceTempView("stu")

    sparkSession.sql("select * from stu where age > 18").show()



    sparkSession.stop()

  }
}
