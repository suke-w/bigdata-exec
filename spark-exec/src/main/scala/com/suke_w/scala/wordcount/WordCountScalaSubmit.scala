package com.suke_w.scala.wordcount

import org.apache.spark.{SparkConf, SparkContext}

object WordCountScalaSubmit {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("WordCountScalaSubmit")
      //.setMaster("local")
    val sc = new SparkContext(conf)

    var inputPath = "D:\\hello.txt"
    if(args.length == 1) {
       inputPath = args(0)
    }
    val dataRDD = sc.textFile(inputPath)
    val flatMapRDD = dataRDD.flatMap(_.split(" "))
    flatMapRDD.map((_,1)).reduceByKey(_ + _).foreach(println(_))

    sc.stop()
  }
}
