package com.suke_w.java.sparksql.load_save;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class LoadAndSaveOpJava {
    public static void main(String[] args) {

        SparkSession sparkSession = SparkSession.builder()
                .appName("LoadAndSaveOpJava")
                .master("local")
                .getOrCreate();

        Dataset<Row> stuDF = sparkSession.read()
                .format("json")
                .load("D:\\Users\\suke_w\\Desktop\\git_repositoy\\123\\bigdata-exec\\spark-exec\\src\\main\\resources\\testFile\\student.text");
        stuDF.select("name","age")
                .write()
                .mode(SaveMode.Append)
                .format("parquet")
                .save("D:\\Users\\suke_w\\Desktop\\git_repositoy\\123\\bigdata-exec\\spark-exec\\src\\main\\resources\\testFile\\student_parquet");

        sparkSession.stop();
    }
}
