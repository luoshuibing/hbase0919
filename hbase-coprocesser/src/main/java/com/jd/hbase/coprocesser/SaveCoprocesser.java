package com.jd.hbase.coprocesser;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;

/**
 * 协处理器
 * 需要将jar包放入hbase的lib目录下
 * @author FM
 * @Description
 * @create 2021-09-21 22:57
 */
public class SaveCoprocesser extends BaseRegionObserver {

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> e, Put put, WALEdit edit, Durability durability) throws IOException {
        //获取表
        Table table = e.getEnvironment().getTable(TableName.valueOf("benefit:student"));
        //增加数据
        table.put(put);
        table.close();
    }
}
