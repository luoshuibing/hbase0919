package com.jd.hbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author FM
 * @Description
 * @create 2021-09-19 0:14
 */
public class TestHbaseApi4 {

    public static void main(String[] args) throws Exception{
        Configuration conf= HBaseConfiguration.create();
        Connection conn = ConnectionFactory.createConnection(conf);
        TableName tn = TableName.valueOf("student");

        Table table = conn.getTable(tn);
        Scan scan = new Scan();
        // scan.addFamily(Bytes.toBytes("job"));
        // scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"));

        BinaryComparator bc = new BinaryComparator(Bytes.toBytes("1001"));
        RegexStringComparator rsc=new RegexStringComparator("\\d{3}");

        Filter rowFilter = new RowFilter(CompareFilter.CompareOp.EQUAL,rsc);
        FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
        list.addFilter(rowFilter);
        // scan.setFilter(rowFilter);
        scan.setFilter(list);
        ResultScanner scanner = table.getScanner(scan);
        for (Result result : scanner) {
            for (Cell cell : result.rawCells()) {
                System.out.println("value:"+ Bytes.toString(CellUtil.cloneValue(cell)));
                System.out.println("rowkey:"+Bytes.toString(CellUtil.cloneRow(cell)));
                System.out.println("family:"+Bytes.toString(CellUtil.cloneFamily(cell)));
                System.out.println("column:"+Bytes.toString(CellUtil.cloneQualifier(cell)));
            }
        }
        table.close();
        conn.close();


    }

}
