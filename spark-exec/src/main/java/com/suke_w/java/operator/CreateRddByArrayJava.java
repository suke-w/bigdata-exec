package com.suke_w.java.operator;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;

import java.util.Arrays;
import java.util.List;

public class CreateRddByArrayJava {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("CreateRddByArrayJava")
                .setMaster("local");

        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        Integer sum = dataRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        System.out.println(sum);

        sc.stop();


    }
}
