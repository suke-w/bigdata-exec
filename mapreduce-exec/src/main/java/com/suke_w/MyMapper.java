package com.suke_w;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class MyMapper extends Mapper<LongWritable, Text,Text,LongWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] strings = value.toString().split(",");
        for (String string : strings) {
            Text k2 = new Text(string);
            LongWritable v2 = new LongWritable(1L);
            context.write(k2,v2);
        }
    }
}
