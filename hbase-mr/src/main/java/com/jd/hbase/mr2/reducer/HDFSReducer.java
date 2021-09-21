package com.jd.hbase.mr2.reducer;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.io.NullWritable;

import java.io.IOException;

/**
 * @author FM
 * @Description
 * @create 2021-09-20 0:51
 */
public class HDFSReducer extends TableReducer<NullWritable, Put,NullWritable> {

    @Override
    protected void reduce(NullWritable key, Iterable<Put> values, Context context) throws IOException, InterruptedException {
        for (Put put : values) {
            context.write(NullWritable.get(),put);
        }
    }
}
