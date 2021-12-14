package com.suke_w.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

/**
 * 全表扫描scan + filter
 */
public class HbaseScanFilterOp {
    public static void main(String[] args) throws IOException {

        //获取配置
        Configuration hbaseConf = HBaseConfiguration.create();
        //指定hbase使用的zookeeper地址，多个用逗号隔开
        hbaseConf.set("hbase.zookeeper.quorum", "bigdata01:2181,bigdata02:2181,bigdata03:2181");
        //指定Hbase在hdfs上的根目录
        hbaseConf.set("hbase.rootdir", "hdfs://bigdata01:9000/hbase");
        //创建Hbase连接，负责对Hbase中的数据做一些增删改查的操作(DML操作)
        Connection conn = ConnectionFactory.createConnection(hbaseConf);

        //指定查询区间，提高查询性能，该区间为左开右闭。
        Scan scan = new Scan().withStartRow(Bytes.toBytes("a")).withStopRow(Bytes.toBytes("f"));
        RowFilter filter = new RowFilter(CompareOperator.LESS_OR_EQUAL, new BinaryComparator(Bytes.toBytes("d")));
        scan.setFilter(filter);
        //添加filter对数据进行过滤
        Table table_s1 = conn.getTable(TableName.valueOf("s1"));

        ResultScanner scanner = table_s1.getScanner(scan);

        for (Result result : scanner) {
            byte[] row = result.getRow();
            List<Cell> cells = result.listCells();
            for (Cell cell : cells) {
                byte[] family = CellUtil.cloneFamily(cell);
                byte[] qualifier = CellUtil.cloneQualifier(cell);
                byte[] value = CellUtil.cloneValue(cell);
                System.out.println("rowkey：" + new String(row) + "列族：" + new String(family) + "列：" + new String(qualifier) + "值：" + new String(value));
            }
        }

    }
}
