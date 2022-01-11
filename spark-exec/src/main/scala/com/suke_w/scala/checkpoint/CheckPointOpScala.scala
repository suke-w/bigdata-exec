package com.suke_w.scala.checkpoint

import org.apache.spark.{SparkConf, SparkContext}

object CheckPointOpScala {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("CheckPointOpScala")
      //.setMaster("local")
    val sc = new SparkContext(conf)

    if(args.length == 0) {
      System.exit(100)
    }
    val outPutPath =args(0)
    sc.setCheckpointDir("hdfs://bigdata01:9000/test/spark/checkpointdir")
    val dataRDD = sc.textFile("hdfs://bigdata01:9000/hello_10000000.dat")
    dataRDD.checkpoint()
    dataRDD.flatMap(_.split(" "))
      .map((_,1))
      .reduceByKey(_ + _)
      .saveAsTextFile(outPutPath)

    sc.stop()
  }
}
