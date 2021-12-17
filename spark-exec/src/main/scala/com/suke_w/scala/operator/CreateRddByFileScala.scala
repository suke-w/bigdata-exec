package com.suke_w.scala.operator

import org.apache.spark.{SparkConf, SparkContext}

object CreateRddByFileScala {
  def main(args: Array[String]): Unit = {


    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("CreateRddByFileScala")

    val sc = SparkContext.getOrCreate(conf)

    val dataRDD = sc.textFile("hdfs://bigdata01:9000/test/hello.txt",2)
    val sum = dataRDD.map(_.length).reduce(_ + _)
    println(sum)


  }
}
