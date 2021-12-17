package com.suke_w.java.sparksql;

import com.suke_w.java.sparksql.pojo.Student;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class Rdd2DataFrameByReflectScala {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .master("local")
                .appName("Rdd2DataFrameByReflectScala")
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());
        Tuple2<String, Integer> jerry = new Tuple2<>("jerry", 35);
        Tuple2<String, Integer> tom = new Tuple2<>("tom", 38);
        JavaRDD<Tuple2<String, Integer>> dataRDD = sc.parallelize(Arrays.asList(jerry, tom));

        JavaRDD<Student> stuRDD = dataRDD.map(new Function<Tuple2<String, Integer>, Student>() {
            @Override
            public Student call(Tuple2<String, Integer> v1) throws Exception {

                return new Student(v1._1, v1._2);
            }
        });

        Dataset<Row> stuDF = sparkSession.createDataFrame(stuRDD,Student.class);

        stuDF.createOrReplaceTempView("student");

        Dataset<Row> resultDF = sparkSession.sql("select * from student where age > 36");

        resultDF.show();

        JavaRDD<Row> resultJavaRDD = resultDF.toJavaRDD();

        List<Student> students = resultJavaRDD.map(new Function<Row, Student>() {
            @Override
            public Student call(Row v1) throws Exception {
                return new Student(v1.getAs("name").toString(), Integer.parseInt(v1.getAs("age").toString()));
            }
        }).collect();
        for (Student student : students) {
            System.out.println(student);
        }


        sparkSession.stop();
    }
}
