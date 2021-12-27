package com.suke_w.java.operator.transformation;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import scala.Tuple3;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

public class TransformationOpJava {
    public static void main(String[] args) {

        JavaSparkContext sc = getSparkContext();

        //mapOp(sc);

        //filterOp(sc);

        //flatMapOp(sc);

        //groupByKeyOp(sc);

        //groupByKeyOp2(sc);

        //reduceByKeyOp(sc);

        //sortByKeyOp(sc);

        //joinOP(sc);

        //distinctOp(sc);

        sc.stop();
    }

    private static void distinctOp(JavaSparkContext sc) {
        Tuple2<Integer,String> t1 = new Tuple2<Integer,String>(150001, "US");
        Tuple2<Integer,String> t2 = new Tuple2<Integer,String>(150002, "CN");
        Tuple2<Integer,String> t3 = new Tuple2<Integer,String>(150003, "CN");
        Tuple2<Integer,String> t4 = new Tuple2<Integer,String>(150004, "IN");
        JavaRDD<Tuple2<Integer, String>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));
        dataRDD.map(new Function<Tuple2<Integer, String>, String>() {
            @Override
            public String call(Tuple2<Integer, String> v1) throws Exception {
                return v1._2;
            }
        }).distinct().foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    private static void joinOP(JavaSparkContext sc) {
        Tuple2<Integer,String> t1 = new Tuple2<Integer,String>(150001, "US");
        Tuple2<Integer,String> t2 = new Tuple2<Integer,String>(150002, "CN");
        Tuple2<Integer,String> t3 = new Tuple2<Integer,String>(150003, "CN");
        Tuple2<Integer,String> t4 = new Tuple2<Integer,String>(150004, "IN");

        Tuple2<Integer,Integer> t5 = new Tuple2<Integer,Integer>(150001, 400);
        Tuple2<Integer,Integer> t6 = new Tuple2<Integer,Integer>(150002, 200);
        Tuple2<Integer,Integer> t7 = new Tuple2<Integer,Integer>(150003, 300);
        Tuple2<Integer,Integer> t8 = new Tuple2<Integer,Integer>(150004, 100);
        JavaRDD<Tuple2<Integer, String>> dataRDD1 = sc.parallelize(Arrays.asList(t1, t2, t3, t4));
        JavaRDD<Tuple2<Integer, Integer>> dataRDD2 = sc.parallelize(Arrays.asList(t5, t6, t7, t8));

        JavaPairRDD<Integer, String> integerStringJavaPairRDD = dataRDD1.mapToPair(new PairFunction<Tuple2<Integer, String>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return new Tuple2<>(integerStringTuple2._1, integerStringTuple2._2);
            }
        });

    }

    private static void sortByKeyOp(JavaSparkContext sc) {
        Tuple2<Integer,Integer> t1 = new Tuple2<Integer,Integer>(150001, 400);
        Tuple2<Integer,Integer> t2 = new Tuple2<Integer,Integer>(150002, 200);
        Tuple2<Integer,Integer> t3 = new Tuple2<Integer,Integer>(150003, 300);
        Tuple2<Integer,Integer> t4 = new Tuple2<Integer,Integer>(150004, 100);
        JavaRDD<Tuple2<Integer, Integer>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));

        dataRDD.mapToPair(new PairFunction<Tuple2<Integer, Integer>, Integer, Integer>() {
            @Override
            public Tuple2<Integer, Integer> call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                return new Tuple2<>(integerIntegerTuple2._2,integerIntegerTuple2._1);
            }
        }).sortByKey(false).foreach(new VoidFunction<Tuple2<Integer, Integer>>() {
            @Override
            public void call(Tuple2<Integer, Integer> integerIntegerTuple2) throws Exception {
                System.out.println(integerIntegerTuple2._2 + ":" + integerIntegerTuple2._1);
            }
        });
    }

    private static void reduceByKeyOp(JavaSparkContext sc) {
        Tuple2<Integer,String> t1 = new Tuple2<Integer,String>(150001, "US");
        Tuple2<Integer,String> t2 = new Tuple2<Integer,String>(150002, "CN");
        Tuple2<Integer,String> t3 = new Tuple2<Integer,String>(150003, "CN");
        Tuple2<Integer,String> t4 = new Tuple2<Integer,String>(150004, "IN");
        JavaRDD<Tuple2<Integer,String>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));
        
        dataRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return new Tuple2<>(integerStringTuple2._2,1);
            }
        }).reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        }).foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
                System.out.println(stringIntegerTuple2._1 + ":" + stringIntegerTuple2._2);
            }
        });
    }

    private static void groupByKeyOp2(JavaSparkContext sc) {
        Tuple3<Integer, String, String> t1 = new Tuple3<Integer, String, String>(150001, "US", "male");
        Tuple3<Integer, String, String> t2 = new Tuple3<Integer, String, String>(150002, "CN", "female");
        Tuple3<Integer, String, String> t3 = new Tuple3<Integer, String, String>(150003, "CN", "male");
        Tuple3<Integer, String, String> t4 = new Tuple3<Integer, String, String>(150004, "IN", "female");
        JavaRDD<Tuple3<Integer, String, String>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));

        dataRDD.mapToPair(new PairFunction<Tuple3<Integer, String, String>, String, Tuple2>() {
            @Override
            public Tuple2<String, Tuple2> call(Tuple3<Integer, String, String> integerStringStringTuple3) throws Exception {
                return new Tuple2<String, Tuple2>(integerStringStringTuple3._2(), new Tuple2(integerStringStringTuple3._1(), integerStringStringTuple3._3()));
            }
        }).groupByKey().foreach(new VoidFunction<Tuple2<String, Iterable<Tuple2>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Tuple2>> stringIterableTuple2) throws Exception {
                String area = stringIterableTuple2._1;
                Iterable<Tuple2> tuple2s = stringIterableTuple2._2;
                System.out.print(area + ":");
                for (Tuple2 tuple2 : tuple2s) {
                    System.out.print("<" + tuple2._1 + "," + tuple2._2 + ">" + " ");
                }
                System.out.println();
            }
        });
    }

    private static void groupByKeyOp(JavaSparkContext sc) {
        Tuple2<Integer, String> t1 = new Tuple2<Integer, String>(150001, "US");
        Tuple2<Integer, String> t2 = new Tuple2<Integer, String>(150002, "CN");
        Tuple2<Integer, String> t3 = new Tuple2<Integer, String>(150003, "CN");
        Tuple2<Integer, String> t4 = new Tuple2<Integer, String>(150004, "IN");

        JavaRDD<Tuple2<Integer, String>> dataRDD = sc.parallelize(Arrays.asList(t1, t2, t3, t4));

        dataRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<Integer, String> integerStringTuple2) throws Exception {
                return new Tuple2<>(integerStringTuple2._2, integerStringTuple2._1);
            }
        }).groupByKey()
                .foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
                    @Override
                    public void call(Tuple2<String, Iterable<Integer>> stringIterableTuple2) throws Exception {
                        String area = stringIterableTuple2._1;
                        System.out.print(area + ":");
                        Iterable<Integer> integers = stringIterableTuple2._2;
                        for (Integer integer : integers) {
                            System.out.print(integer + " ");
                        }
                        System.out.println();
                    }
                });
    }

    /**
     * flatmao操作
     *
     * @param sc
     */
    private static void flatMapOp(JavaSparkContext sc) {
        JavaRDD<String> dataRDD = sc.parallelize(Arrays.asList("good good study", "day day up"));
        dataRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] strings = s.split(" ");
                return Arrays.asList(strings).iterator();
            }
        }).foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });
    }

    /**
     * filter操作
     *
     * @param sc
     */
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
     *
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
     *
     * @return
     */
    private static JavaSparkContext getSparkContext() {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("TransformationOpJava");
        return new JavaSparkContext(conf);
    }
}
