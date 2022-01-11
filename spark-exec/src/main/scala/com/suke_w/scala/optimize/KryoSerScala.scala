package com.suke_w.scala.optimize

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

/**
 * spark调优：高性能序列化类库
 * kryo:31B
 * java:148B
 */
object KryoSerScala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("KryoSerScala")
      .setMaster("local")
      /**
       * 指定使用kryo序列化机制
       * 注意：如果使用了registerKryoClasses，该行设置可以省略
       */
      .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
      /** 注册自定义的数据类型 */
      //.registerKryoClasses(Array(classOf[Person]))

    val sc = new SparkContext(conf)

    val dataRDD = sc.parallelize(Array("hello you", "hello me"))
    val flatRDD = dataRDD.flatMap(_.split(" "))
    val personRDD = flatRDD.map(word => Person(word, 19)).persist(StorageLevel.MEMORY_ONLY_SER)
    personRDD.foreach(println(_))

    while (true) {

    }

  }
}
case class Person(name: String,age: Int) extends Serializable
