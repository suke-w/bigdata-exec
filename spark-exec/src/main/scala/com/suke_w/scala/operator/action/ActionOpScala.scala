package com.suke_w.scala.operator.action

import org.apache.spark.{SparkConf, SparkContext}

object ActionOpScala {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = getSparkContext

    val dataRDD = sc.parallelize(Array(1, 2, 3, 4, 5))
    dataRDD.reduce(_ + _)


    sc.stop()
  }

  private def getSparkContext = {
    val conf = new SparkConf().setMaster("local").setAppName("ActionOpScala")
    val sc = new SparkContext(conf)
    sc
  }
}
