package com.suke_w.java.sparksql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple3;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Rdd2DataFrameByProgrameScala {
    public static void main(String[] args) {
        SparkSession sparkSession = SparkSession.builder()
                .appName("Rdd2DataFrameByProgrameScala")
                .master("local")
                .getOrCreate();

        JavaSparkContext sc = JavaSparkContext.fromSparkContext(sparkSession.sparkContext());

        Tuple3<String, String, Integer> zhangsan = new Tuple3<>("zhangsan", "swim", 32);
        Tuple3<String, String, Integer> lisi = new Tuple3<>("lisi", "run", 28);
        Tuple3<String, String, Integer> wangwu = new Tuple3<>("wangwu", "football", 66);

        JavaRDD<Tuple3<String, String, Integer>> dataRDD = sc.parallelize(Arrays.asList(zhangsan, lisi, wangwu));

        JavaRDD<Row> rowRDD = dataRDD.map(new Function<Tuple3<String, String, Integer>, Row>() {
            @Override
            public Row call(Tuple3<String, String, Integer> v1) throws Exception {
                return RowFactory.create(v1._1(), v1._2(), v1._3());
            }
        });

        ArrayList<StructField> structFields = new ArrayList<>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("game",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        StructType schema = DataTypes.createStructType(structFields);

        Dataset<Row> dataDF = sparkSession.createDataFrame(rowRDD, schema);
        dataDF.createOrReplaceTempView("student");
        Dataset<Row> resultDF = sparkSession.sql("select * from student where name = 'zhangsan'");
        JavaRDD<Row> resultJavaRDD = resultDF.javaRDD();

        List<Tuple3<String, String, Integer>> collect = resultJavaRDD.map(new Function<Row, Tuple3<String, String, Integer>>() {

            @Override
            public Tuple3<String, String, Integer> call(Row v1) throws Exception {
                return new Tuple3<>(v1.getAs("name".toString()), v1.getAs("game").toString(), Integer.parseInt(v1.getAs("age").toString()));
            }
        }).collect();

        for (Tuple3<String, String, Integer> stringStringIntegerTuple3 : collect) {
            System.out.println(stringStringIntegerTuple3);
        }


    }
}
