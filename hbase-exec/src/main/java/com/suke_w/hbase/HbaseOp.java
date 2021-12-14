package com.suke_w.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * 操作Hbase
 * 表：创建删除
 * 数据：增、删、改、查
 */
public class HbaseOp {
    public static void main(String[] args) throws IOException {
        //获取连接
        Connection conn = getConn();
        //添加数据/更新数据
        //put(conn);
        //查询数据
        //get(conn);
        //查询多版本数据
        //getMultiVersion(conn);
        //删除数据
        //delete(conn);
        //创建表
        //createTable(conn);
        //删除表
        dropTable(conn);
        //关闭连接
        conn.close();

    }

    /**
     * 删除表
     * @param conn
     * @throws IOException
     */
    private static void dropTable(Connection conn) throws IOException {
        Admin admin = conn.getAdmin();
        admin.disableTable(TableName.valueOf("test"));
        admin.deleteTable(TableName.valueOf("test"));
    }

    /**
     * 创建表
     * @param conn
     * @throws IOException
     */
    private static void createTable(Connection conn) throws IOException {
        Admin admin = conn.getAdmin();

        ColumnFamilyDescriptor level = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("level"))
                .setMaxVersions(2)
                .build();
        ColumnFamilyDescriptor info = ColumnFamilyDescriptorBuilder.newBuilder(Bytes.toBytes("info"))
                .setMaxVersions(1)
                .build();
        ArrayList<ColumnFamilyDescriptor> families = new ArrayList<ColumnFamilyDescriptor>();
        families.add(level);
        families.add(info);
        TableDescriptor desc = TableDescriptorBuilder.newBuilder(TableName.valueOf("test"))
                .setColumnFamilies(families)
                .build();

        admin.createTable(desc);
    }


    /**
     * 删除数据
     * @param conn
     * @throws IOException
     */
    private static void delete(Connection conn) throws IOException {
        Table table_student = conn.getTable(TableName.valueOf("student"));

        Delete delete = new Delete(Bytes.toBytes("laowang"));

        //delete.addColumn(Bytes.toBytes("level"),Bytes.toBytes("class"));

        table_student.delete(delete);

        table_student.close();
    }

    /**
     * 查询多版本数据
     * @param conn
     * @throws IOException
     */
    private static void getMultiVersion(Connection conn) throws IOException {
        Table table_student = conn.getTable(TableName.valueOf("student"));
        Get get = new Get(Bytes.toBytes("laowang"));
        //get.readAllVersions();
        get.readVersions(2);
        Result result = table_student.get(get);
        List<Cell> columnCells = result.getColumnCells(Bytes.toBytes("info"), Bytes.toBytes("age"));
        for (Cell cell : columnCells) {
            byte[] value = CellUtil.cloneValue(cell);
            long timestamp = cell.getTimestamp();
            System.out.println("值：" + new String(value) + "时间戳：" + timestamp);
        }
        table_student.close();
    }

    /**
     * 查询数据
     *
     * @param conn
     * @throws IOException
     */
    private static void get(Connection conn) throws IOException {
        //获取Table，指定要操作的表名，表需要提前创建好
        Table studentT = conn.getTable(TableName.valueOf("student"));
        //指定Rowkey，返回Get对象
        Get studentTGet = new Get(Bytes.toBytes("laowang"));
        //指定要查询当前Rowkey数据哪些列族中的列，若不指定，默认查询当前Rowkey所有列的内容
        studentTGet.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"));
        studentTGet.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"));

        Result result = studentT.get(studentTGet);
        //如果不清楚Hbase中到底有哪些列族和列，可以使用listCells()获取所有cell(单元格)，cell对应哪个某一列数据
        List<Cell> cells = result.listCells();
        for (Cell cell : cells) {
            byte[] family = CellUtil.cloneFamily(cell);
            byte[] qualifier = CellUtil.cloneQualifier(cell);
            byte[] value = CellUtil.cloneValue(cell);
            System.out.println("列族：" + new String(family) + "，列：" + new String(qualifier) + "，值：" + new String(value));
        }

        byte[] value = result.getValue(Bytes.toBytes("info"), Bytes.toBytes("age"));
        System.out.println("age：" + new String(value));

        studentT.close();
    }

    /**
     * 添加数据
     *
     * @param conn
     * @throws IOException
     */
    private static void put(Connection conn) throws IOException {
        //获取Table，指定要操作的表名，表需要提前创建好
        Table studentT = conn.getTable(TableName.valueOf("student"));
        //指定Rowkey，反回put对象
        Put studentTPut = new Put(Bytes.toBytes("laowang"));
        //put对象中指定列族、列、值
        //put 'student','laowang','info:age','18'
        studentTPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("23"));
        //put 'student','laowang','info:sex','man'
        studentTPut.addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes("man"));
        //put 'student','laowang','level:class','A'
        studentTPut.addColumn(Bytes.toBytes("level"), Bytes.toBytes("class"), Bytes.toBytes("A"));

        studentT.put(studentTPut);
        //关闭表连接
        studentT.close();
    }

    /**
     * 获取连接
     *
     * @return
     * @throws IOException
     */
    private static Connection getConn() throws IOException {
        //获取配置
        Configuration hbaseConf = HBaseConfiguration.create();
        //指定hbase使用的zookeeper地址，多个用逗号隔开
        hbaseConf.set("hbase.zookeeper.quorum", "bigdata01:2181,bigdata02:2181,bigdata03:2181");
        //指定Hbase在hdfs上的根目录
        hbaseConf.set("hbase.rootdir", "hdfs://bigdata01:9000/hbase");
        //创建Hbase连接，负责对Hbase中的数据做一些增删改查的操作(DML操作)
        return ConnectionFactory.createConnection(hbaseConf);
    }
}
