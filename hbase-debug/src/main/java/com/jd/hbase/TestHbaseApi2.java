package com.jd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author FM
 * @Description
 * @create 2021-09-19 0:14
 */
public class TestHbaseApi2 {

    public static void main(String[] args) throws Exception{
        Configuration configuration = HBaseConfiguration.create();
        Connection connection = ConnectionFactory.createConnection(configuration);
        TableName tableName = TableName.valueOf("benefit:emp");
        Admin admin = connection.getAdmin();
        // if(admin.tableExists(tableName)){
        //     // Table table = connection.getTable(tableName);
        //     admin.disableTable(tableName);
        //     admin.deleteTable(tableName);
        // }


        // Table table = connection.getTable(tableName);
        // String rowkey="1001";
        // Delete delete = new Delete(Bytes.toBytes(rowkey));
        // table.delete(delete);

        Table table = connection.getTable(tableName);
        Scan scan = new Scan();
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("value:"+ Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("rowkey:"+CellUtil.cloneRow(cell));
                System.out.println("family:"+CellUtil.cloneFamily(cell));
                System.out.println("column:"+CellUtil.cloneQualifier(cell));
            }
        }


    }

}
