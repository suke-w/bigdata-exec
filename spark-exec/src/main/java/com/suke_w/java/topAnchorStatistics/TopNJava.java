package com.suke_w.java.topAnchorStatistics;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
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

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class TopNJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TopNJava");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> gift_recordRDD = jsc.textFile("D:\\gift_record.log");
        JavaRDD<String> video_infoRDD = jsc.textFile("D:\\video_info.log");

        JavaPairRDD<String, Tuple2<String,String>> video_info_field_RDD = video_infoRDD.mapToPair(new PairFunction<String, String, Tuple2<String,String>>() {
            @Override
            public Tuple2<String, Tuple2<String,String>> call(String s) throws Exception {
                JSONObject jsonObject = JSON.parseObject(s);
                String uid = jsonObject.getString("uid");
                String vid = jsonObject.getString("vid");
                String area = jsonObject.getString("area");
                Tuple2<String, String> stringStringTuple2 = new Tuple2<>(uid, area);
                return new Tuple2<String, Tuple2<String,String>>(vid, stringStringTuple2);
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

        //video_info_field_RDD.foreach(new VoidFunction<Tuple2<String, Tuple2>>() {
        //    @Override
        //    public void call(Tuple2<String, Tuple2> stringTuple2Tuple2) throws Exception {
        //        System.out.println(stringTuple2Tuple2);
        //    }
        //});
        //gift_record_goldsumRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
        //    @Override
        //    public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
        //        System.out.println(stringIntegerTuple2);
        //    }
        //});


        //需要用到map的地方替换为mapToPair
        JavaPairRDD<Tuple2<String, String>, Integer> joinMapRDD = video_info_field_RDD.join(gift_record_goldsumRDD).mapToPair(
                new PairFunction<Tuple2<String, Tuple2<Tuple2<String, String>, Integer>>, Tuple2<String, String>, Integer>() {
                    @Override
                    public Tuple2<Tuple2<String, String>, Integer> call(Tuple2<String, Tuple2<Tuple2<String, String>, Integer>> stringTuple2Tuple2) throws Exception {
                        String uid = stringTuple2Tuple2._2._1._1;
                        String area = stringTuple2Tuple2._2._1._2;
                        Integer gold_sum = stringTuple2Tuple2._2._2;

                        return new Tuple2<Tuple2<String, String>, Integer>(new Tuple2<String, String>(uid, area), gold_sum);
                    }
                }
        );

        JavaPairRDD<String, Iterable<Tuple2<String, Integer>>> groupRDD = joinMapRDD.mapToPair(new PairFunction<Tuple2<Tuple2<String, String>, Integer>, String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Tuple2<String, Integer>> call(Tuple2<Tuple2<String, String>, Integer> tup) throws Exception {
                return new Tuple2<String, Tuple2<String, Integer>>(tup._1._2, new Tuple2<String, Integer>(tup._1._1, tup._2));
            }
        }).groupByKey();


        JavaRDD<Tuple2<String,String>> top3RDD = groupRDD.map(new Function<Tuple2<String, Iterable<Tuple2<String, Integer>>>, Tuple2<String,String>>() {
            @Override
            public Tuple2<String, String> call(Tuple2<String, Iterable<Tuple2<String, Integer>>> v1) throws Exception {
                String area = v1._1;
                ArrayList<Tuple2<String, Integer>> tuple2Lists = Lists.newArrayList(v1._2);
                Collections.sort(tuple2Lists, new Comparator<Tuple2<String, Integer>>() {
                    @Override
                    public int compare(Tuple2<String, Integer> o1, Tuple2<String, Integer> o2) {
                        return o2._2 - o1._2;
                    }
                });

                StringBuffer sb = new StringBuffer();
                for (int i = 0; i < tuple2Lists.size(); i++) {
                    if (i < 3) {
                        Tuple2<String, Integer> tuple2 = tuple2Lists.get(i);
                        if (i != 0) {
                            sb.append(",");
                        }
                        sb.append(tuple2._1 + ":" + tuple2._2);
                    }
                }
                return new Tuple2<String, String>(area, sb.toString());
            }
        });

        top3RDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> stringStringTuple2) throws Exception {
                System.out.println(stringStringTuple2);
            }
        });

        jsc.stop();

    }
}
