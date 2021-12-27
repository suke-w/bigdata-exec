package com.suke_w.java.topAnchorStatistics;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.json4s.jackson.Json;
import scala.Tuple2;

public class TopNJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TopNJava");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> gift_recordRDD = jsc.textFile("D:\\gift_record.log");
        JavaRDD<String> video_infoRDD = jsc.textFile("D:\\video_info.log");

        JavaPairRDD<String, Tuple2> video_info_field_RDD = video_infoRDD.mapToPair(new PairFunction<String, String, Tuple2>() {
            @Override
            public Tuple2<String, Tuple2> call(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String uid = jsonObject.getString("uid");
                String vid = jsonObject.getString("vid");
                String area = jsonObject.getString("area");
                Tuple2<String, String> stringStringTuple2 = new Tuple2<>(vid, area);
                return new Tuple2<String, Tuple2>(uid, stringStringTuple2);
            }
        });


        JavaPairRDD<String, Integer> gift_record_fieldRDD = gift_recordRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String vid = jsonObject.getString("vid");
                Integer gold = Integer.parseInt(jsonObject.getString("gold"));
                return new Tuple2<>(vid, gold);
            }
        });


        JavaPairRDD<String, Integer> gift_record_goldsumRDD = gift_record_fieldRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        video_info_field_RDD.join(gift_record_goldsumRDD).mapToPair(new PairFunction<Tuple2<String, Tuple2<Tuple2, Integer>>, Tuple2<String, String>, Integer>() {
            @Override
            public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, Tuple2<Tuple2, Integer>> stringTuple2Tuple2) throws Exception {

                return null;
            }
        });

        //gift_record_field_goldsumRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
        //    @Override
        //    publicT void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
        //        System.out.println(stringIntegerTuple2);
        //    }
        //});




        jsc.stop();

    }
}
