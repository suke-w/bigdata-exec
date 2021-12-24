package com.suke_w.scala.operator.transformation

import org.apache.spark.{SparkConf, SparkContext}

object TransformationOpScala {
  def main(args: Array[String]): Unit = {

    val sc: SparkContext = getSparkContext

    //mapOp(sc)

    //filterOp(sc)

    //flatMapOp(sc)

    //groupByKeyOp(sc)

    //groupByKeyOpTuple3(sc)

    //reduceByKeyOp(sc)

    //sortByKeyOp(sc)

    //joinOp(sc)

    distinctOp(sc)

    sc.stop()
  }

  private def distinctOp(sc: SparkContext) = {
    val dataRDD = sc.parallelize(Array((150001, "US"), (150002, "CN"), (150003, "CN"), (150004, "IN")))
    dataRDD.map(_._2).distinct().foreach(println(_))
    dataRDD.distinct().foreach(println(_))
  }

  private def joinOp(sc: SparkContext) = {
    val dataRDD1 = sc.parallelize(Array((150001, "US"), (150002, "CN"), (150003, "CN"), (150004, "IN")))
    val dataRDD2 = sc.parallelize(Array((150001, 400), (150002, 200), (150003, 300), (150004, 100)))
    val joinRDD = dataRDD1.join(dataRDD2)
    joinRDD.foreach(tup => {
      val uid = tup._1
      val area_gold = tup._2

      val area = area_gold._1
      val gold = area_gold._2

      println(uid + "\t" + area + "\t" + gold)
    })
  }

  private def sortByKeyOp(sc: SparkContext) = {
    val dataRDD = sc.parallelize(Array((150001, 400), (150002, 200), (150003, 300), (150004, 100)))
    dataRDD.map(tup => (tup._2, tup._1)).sortByKey(false).foreach(println(_))
    dataRDD.sortBy(tup => (tup._2), false).foreach(println(_))
  }

  private def reduceByKeyOp(sc: SparkContext) = {
    val dataRDD = sc.parallelize(Array((150001, "US"), (150002, "CN"), (150003, "CN"), (150004, "IN")))
    dataRDD.map(tup => (tup._2, 1)).reduceByKey(_ + _).foreach(println(_))

  }

  private def groupByKeyOp(sc: SparkContext) = {
    val dataRDD = sc.parallelize(Array((150001, "US"), (150002, "CN"), (150003, "CN"), (150004, "IN")))
    dataRDD.map(tup => (tup._2, tup._1))
      .groupByKey()
      .foreach(
        tup => {
          val aera = tup._1
          print(aera + ": ")
          for (uid <- tup._2) {
            print(uid + " ")
          }
          println()
        }
      )
  }

  private def groupByKeyOpTuple3(sc: SparkContext) = {
    val dataRDD = sc.parallelize(Array((150001, "US", "male"), (150002, "CN", "female"), (150003, "CN", "male"), (150004, "IN", "female")))
    dataRDD.map(tup => (tup._2, (tup._1, tup._3)))
      .groupByKey()
      .foreach(
        tup => {
          val aera = tup._1
          print(aera + "：")
          val it = tup._2
          for ((uid, sex) <- it) {
            print("<" + uid + "," + sex + "> ")
          }
          println()
        }
      )
  }

  private def flatMapOp(sc: SparkContext) = {
    val dataRDD = sc.parallelize(Array("good good study", "day day up"))
    dataRDD.flatMap(_.split(" ")).foreach(println(_))
  }

  private def filterOp(sc: SparkContext) = {
    val dataRDD = sc.parallelize(Array(1, 2, 3, 4, 5))
    dataRDD.filter(_ % 2 == 0).foreach(println(_))
  }

  private def mapOp(sc: SparkContext) = {
    val dataRDD = sc.parallelize(Array(1, 2, 3, 4, 5))
    dataRDD.map(_ * 2).foreach(println(_))
  }

  private def getSparkContext = {
    val conf = new SparkConf()
      .setMaster("local")
      .setAppName("TransformationOpScala")

    val sc = new SparkContext(conf)
    sc
  }
}
