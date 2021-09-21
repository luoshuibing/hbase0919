package com.jd.hbase.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author FM
 * @Description
 * @create 2021-09-19 18:11
 */
public class HBaseUtil {

    private static final ThreadLocal<Connection> TL = new ThreadLocal<>();

    private HBaseUtil() {

    }

    public static void makeHBaseConnection() throws Exception {
        Connection conn = TL.get();
        if (conn == null) {
            Configuration conf = HBaseConfiguration.create();
            conn = ConnectionFactory.createConnection(conf);
            TL.set(conn);
        }
    }

    public static void saveData(String tableName, String rowkey, String family, String column, String value) throws Exception {
        Connection conn = TL.get();
        Table table = conn.getTable(TableName.valueOf(tableName));
        Put put = new Put(Bytes.toBytes(rowkey));
        put.addColumn(Bytes.toBytes(family), Bytes.toBytes(column), Bytes.toBytes(value));
        table.put(put);
        table.close();
    }

    public static void close() throws Exception {
        Connection conn = TL.get();
        if (conn != null) {
            conn.close();
            TL.remove();
        }
    }

    public static String genRegionNum(String rowkey, int regionCount) {
        int regionNum;
        int hash = rowkey.hashCode();
        if (regionCount > 0 && (regionCount & (regionCount - 1)) == 0) {
            regionNum = hash & (regionCount - 1);
        } else {
            regionNum = hash % regionCount;
        }
        return regionNum + "_" + rowkey;
    }

    public static byte[][] genRegionKeys(int regionCount) {
        byte[][] bs = new byte[regionCount - 1][];
        for (int i = 0; i < regionCount - 1; i++) {
            bs[i] = Bytes.toBytes(i + "|");
        }
        return bs;
    }

}
