package com.suke_w.scala.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCountScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setMaster("local").setAppName("WordCountScala")
    val sc = new SparkContext(conf)


    val dataRDD = sc.textFile("D:\\hello.txt")
    val flatMapRDD = dataRDD.flatMap(_.split(" "))
    flatMapRDD.map((_,1)).reduceByKey(_ + _).foreach(println(_))

    sc.stop()
  }
}
