package com.jd.hbase;

import com.jd.hbase.util.HBaseUtil;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;

/**
 * @author FM
 * @Description
 * @create 2021-09-19 0:14
 */
public class TestHbaseApi5 {


    public static void main(String[] args) throws Exception{
        BasicConfigurator.configure();
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        // Admin admin = connection.getAdmin();
        // System.out.println(connection);
        // HTableDescriptor td = new HTableDescriptor(TableName.valueOf("emp1"));
        // HColumnDescriptor cd = new HColumnDescriptor("info");
        // td.addFamily(cd);
        // byte[][] bs=new byte[2][];
        // bs[0]= Bytes.toBytes("0|");
        // bs[1]= Bytes.toBytes("1|");
        // admin.createTable(td,bs);
        // System.out.println("表格创建成功");
        Table empTable = connection.getTable(TableName.valueOf("emp1"));
        String rowkey="zhangsan";
        rowkey = HBaseUtil.genRegionNum(rowkey, 3);
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes("zhangsan"));
        empTable.put(put);
    }

}
