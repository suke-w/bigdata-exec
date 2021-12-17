package com.suke_w.scala.operator

import org.apache.spark.{SparkConf, SparkContext}

object CreateRddByArrayScala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local")
      .setAppName("CreateRddByArrayScala")
    val sc = new SparkContext(conf)

    val arr = Array(1, 2, 3, 4, 5)
    val dataRDD = sc.parallelize(arr)

    val i = dataRDD.reduce(_ + _)

    println(i)

    sc.stop()


  }
}
