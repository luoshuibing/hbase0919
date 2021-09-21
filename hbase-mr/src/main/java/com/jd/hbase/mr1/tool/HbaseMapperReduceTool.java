package com.jd.hbase.mr1.tool;

import com.jd.hbase.mr1.mapper.ScanDataMapper;
import com.jd.hbase.mr1.reducer.SaveDataReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.util.Tool;

/**
 * @author FM
 * @Description
 * @create 2021-09-19 23:13
 */
public class HbaseMapperReduceTool extends Configured implements Tool {

    @Override
    public int run(String[] strings) throws Exception {
        Job job=Job.getInstance();

        job.setJarByClass(HbaseMapperReduceTool.class);

        TableMapReduceUtil.initTableMapperJob(
                "student",
                new Scan(),
                ScanDataMapper.class,
                ImmutableBytesWritable.class,
                Put.class,
                job
        );

        TableMapReduceUtil.initTableReducerJob(
                "benefit:emp",
                SaveDataReducer.class,
                job
        );

        boolean flg = job.waitForCompletion(true);

        return flg? JobStatus.State.SUCCEEDED.getValue():JobStatus.State.FAILED.getValue();
    }

}
