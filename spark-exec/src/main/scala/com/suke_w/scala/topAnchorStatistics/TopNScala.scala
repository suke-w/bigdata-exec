package com.suke_w.scala.topAnchorStatistics

import com.alibaba.fastjson.JSON
import org.apache.spark.{SparkConf, SparkContext}

object TopNScala {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().setMaster("local").setAppName("TopNScala")
    val sc = new SparkContext(conf)

    val gift_recordRDD = sc.textFile("D:\\gift_record.log")
    val video_infoRDD = sc.textFile("D:\\video_info.log")


    val video_info_field_RDD = video_infoRDD.map(tup => {
      val video_info_object = JSON.parseObject(tup)
      val uid = video_info_object.getString("uid")
      val vid = video_info_object.getString("vid")
      val area = video_info_object.getString("area")
      (vid, (uid, area))
    })//.foreach(println(_))
    video_info_field_RDD.foreach(println(_))
    val gift_record_fieldRDD = gift_recordRDD.map(tup => {
      val gift_record_object = JSON.parseObject(tup)
      val vid = gift_record_object.getString("vid")
      val gold = gift_record_object.getString("gold")
      (vid, gold)
    })//.foreach(println(_))


    val gift_record_fieldAggRDD = gift_record_fieldRDD.reduceByKey(_ + _)
    gift_record_fieldAggRDD.foreach(println(_))
    //(14943445328940019,((8407173251010,ID),2030))
    val joinRDD = video_info_field_RDD.join(gift_record_fieldAggRDD)

    /** 查重 */
    //joinRDD.map(tup => {
    //  (tup._1,1)
    //}).reduceByKey(_ + _).filter(_._2 == 1).foreach(println(_))

    //同一主播可能有不同直播间，vid不同
    //((8407173251015,US),8100)
    val gold_sum_allRDD = joinRDD.map(tup => {
      val tmp = tup._2
      (tmp._1, tmp._2.toInt)
    }).reduceByKey(_ + _)

    //(CN,CompactBuffer((8407173251008,10020), (8407173251003,5010), (8407173251014,2030), (8407173251006,1020), (8407173251011,1020)))
    val groupRDD = gold_sum_allRDD.map(tup => {
      val tmp = tup._1
      (tmp._2, (tmp._1, tup._2))
    }).groupByKey()

    val top3RDD = groupRDD.map(tup => {
      val area = tup._1
      val top3 = tup._2.toList.sortBy(_._2).reverse.take(3).map(tup => tup._1 + ":" + tup._2).mkString(",")
      (area, top3)
    })

    top3RDD.foreach(tup=>println(tup._1+"\t"+tup._2))

    sc.stop()
  }
}
