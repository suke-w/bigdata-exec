package com.suke_w.java.topAnchorStatistics;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import scala.Tuple2;

public class TopNJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TopNJava");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> gift_recordRDD = jsc.textFile("D:\\gift_record.log");
        JavaRDD<String> video_infoRDD = jsc.textFile("D:\\video_info.log");

        JavaRDD<Object> video_info_field_RDD = video_infoRDD.map(new Function<String, Object>() {
            @Override
            public Object call(String v1) throws Exception {
                JSONObject jsonObject = JSON.parseObject(v1);
                String uid = jsonObject.getString("uid");
                String vid = jsonObject.getString("vid");
                String area = jsonObject.getString("area");
                return new Tuple2<String, Tuple2>(uid, new Tuple2(vid, area));
            }
        });


        JavaRDD<Object> gift_record_fieldRDD = gift_recordRDD.map(new Function<String, Object>() {
            @Override
            public Object call(String v1) throws Exception {

                JSONObject jsonObject = JSON.parseObject(v1);
                String vid = jsonObject.getString("vid");
                String gold = jsonObject.getString("gold");
                return new Tuple2<String, String>(vid, gold);

            }
        });



    }
}
