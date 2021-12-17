package com.suke_w.java.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

public class CreateRddByFileScala {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("CreateRddByFileScala")
                .setMaster("local");
        JavaSparkContext sc = new JavaSparkContext(conf);

        JavaRDD<String> dataRDD = sc.textFile("hdfs://bigdata01:9000/test/hello.txt", 2);

        Integer sum = dataRDD.map(new Function<String, Integer>() {
            @Override
            public Integer call(String v1) throws Exception {
                return v1.length();
            }
        }).reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        System.out.println(sum);

        sc.stop();
    }
}
