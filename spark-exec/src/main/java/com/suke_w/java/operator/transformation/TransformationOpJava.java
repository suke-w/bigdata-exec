package com.suke_w.java.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class TransformationOpJava {
    public static void main(String[] args) {

        JavaSparkContext sc = getSparkContext();

        //mapOp(sc);
        //filterOp(sc);
        //
        Tuple2<Integer, String> t1 = new Tuple2<Integer, String>(150001,"US");
        Tuple2<Integer, String> t2 = new Tuple2<Integer, String>(150002, "CN");
        Tuple2<Integer,String> t3 = new Tuple2<Integer,String>(150003, "CN");
        Tuple2<Integer,String> t4 = new Tuple2<Integer,String>(150004, "IN");

        JavaRDD<Tuple2<Integer, String>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));




        sc.stop();
    }

    private static void filterOp(JavaSparkContext sc) {
        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));
        JavaRDD<Integer> filterRDD = dataRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1 % 2 == 0;
            }
        });
        filterRDD.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    /**
     * map操作
     * @param sc
     */
    private static void mapOp(JavaSparkContext sc) {
        JavaRDD<Integer> dataRDD = sc.parallelize(Arrays.asList(1, 2, 3, 4, 5));

        dataRDD.map(
                new Function<Integer, Integer>() {
                    @Override
                    public Integer call(Integer v1) throws Exception {
                        return v1 * 2;
                    }
                }
        ).foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
    }

    /**
     * 获取sparkcontext
     * @return
     */
    private static JavaSparkContext getSparkContext() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TransformationOpJava");
        return new JavaSparkContext(conf);
    }
}
