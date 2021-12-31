package com.suke_w.java.wordcount;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Iterator;

public class WordCountJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setAppName("WordCountJava").setMaster("local");
        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> dataRDD = jsc.textFile("D:\\hello.txt");

        JavaRDD<String> flatMapRDD = dataRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] strings = s.split(" ");
                Iterator<String> iterator = Arrays.asList(strings).iterator();
                return iterator;
            }
        });

        JavaPairRDD<String, Integer> pairRDD = flatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s, 1);
            }
        });

        JavaPairRDD<String, Integer> reduceRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        reduceRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2);
            }
        });
    }
}
