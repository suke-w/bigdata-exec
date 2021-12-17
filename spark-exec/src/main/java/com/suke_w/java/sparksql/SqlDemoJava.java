package com.suke_w.java.sparksql;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.RelationalGroupedDataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class SqlDemoJava {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf();

        SparkSession sparkSession = SparkSession.builder()
                .appName("SqlDemoScala")
                .master("local")
                .config(conf)
                .getOrCreate();

        Dataset<Row> dataset_student = sparkSession.read().json("D:\\student.text");

        /**
         * DataFrame的算子操作
         */
        //dataset_student.show();
        RelationalGroupedDataset ageGroup = dataset_student.groupBy("age");
        Dataset<Row> count = ageGroup.count();
        //count.show();

        //dataset_student.filter(col("age").gt("18")).show();
        //dataset_student.where(col("age").gt("18")).show();
        
        //dataset_student.select(col("anme"),col("age").plus(1));

        /**
         * DataFrame的sql操作
         */
        dataset_student.createOrReplaceTempView("stu");
        sparkSession.sql("select * from stu where age > 18").show();
        sparkSession.stop();

    }
}
