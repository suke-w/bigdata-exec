package com.suke_w.scala.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object ActionOpScala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("ActionOpScala")
    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Array(1, 2, 3, 4, 5))

    dataRDD.reduce(_ + _)
  }
}
