package com.jd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.BasicConfigurator;

/**
 * @author FM
 * @Description
 * @create 2021-09-19 0:14
 */
public class TestHbaseApi1 {

    public static void main(String[] args) throws Exception{
        BasicConfigurator.configure();
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        Admin admin = connection.getAdmin();
        try{
            NamespaceDescriptor namespace = admin.getNamespaceDescriptor("negative");
        }catch(NamespaceNotFoundException e){
            admin.createNamespace(NamespaceDescriptor.create("negative").build());
        }
        TableName tn = TableName.valueOf("negative:tn");
        boolean result = admin.tableExists(tn);
        System.out.println(result);
        if(result){
            Table table = connection.getTable(tn);
            String rowkey = "1001";
            Get get = new Get(Bytes.toBytes(rowkey));
            Result r = table.get(get);
            boolean empty = r.isEmpty();
            System.out.println(empty);
            if(empty){
                Put put = new Put(Bytes.toBytes(rowkey));
                String family = "info";
                String column = "name";
                String value = "zhangsan";
                put.addColumn(Bytes.toBytes(family),Bytes.toBytes(column),Bytes.toBytes(value));
                table.put(put);
                System.out.println("增加数据");
            }else{
                for (Cell cell : r.rawCells()) {
                    System.out.println("value:"+Bytes.toString(CellUtil.cloneValue(cell)));
                    System.out.println("rowkey:"+CellUtil.cloneRow(cell));
                    System.out.println("family:"+CellUtil.cloneFamily(cell));
                    System.out.println("column:"+CellUtil.cloneQualifier(cell));
                }
            }
        }else{

            HTableDescriptor td=new HTableDescriptor(tn);
            td.addCoprocessor("com.jd.hbase.coprocesser.SaveCoprocesser");
            HColumnDescriptor hColumnDescriptor = new HColumnDescriptor("info");
            td.addFamily(hColumnDescriptor);
            admin.createTable(td);
            System.out.println("表格创建成功");
        }

    }

}
