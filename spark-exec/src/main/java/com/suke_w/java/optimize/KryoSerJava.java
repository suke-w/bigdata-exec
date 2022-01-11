package com.suke_w.java.optimize;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;

public class KryoSerJava {
    public static void main(String[] args) {

        SparkConf conf = new SparkConf()
                .setAppName("KryoSerJava")
                .setMaster("local");
                //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
                //.set("spark.kryo.classesToRegister", "com.suke_w.java.optimize.Person");

        JavaSparkContext jsc = new JavaSparkContext(conf);

        JavaRDD<String> dataRDD = jsc.parallelize(Arrays.asList("hello you", "hello me"));

        JavaRDD<String> wordsRDD = dataRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterator<String> call(String s) throws Exception {
                String[] strings = s.split(" ");
                return Arrays.asList(strings).iterator();
            }
        });

        JavaRDD<Person> personRDD = wordsRDD.map(new Function<String, Person>() {
            @Override
            public Person call(String v1) throws Exception {
                return new Person(v1, 19);
            }
        }).persist(StorageLevel.MEMORY_ONLY_SER());

        personRDD.foreach(new VoidFunction<Person>() {
            @Override
            public void call(Person person) throws Exception {
                System.out.println(person);
            }
        });

        while (true) {

        }


    }
}

class Person implements Serializable {
    private String name;
    private int age;

    Person(String name, int age) {
        this.name = name;
        this.age = age;
    }

    @Override
    public String toString() {
        return "Person{" +
                "name='" + name + '\'' +
                ", age=" + age +
                '}';
    }
}
