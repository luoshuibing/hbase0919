package com.jd.hbase.mr2;

import com.jd.hbase.mr2.mapper.HDFSMapper;
import com.jd.hbase.mr2.reducer.HDFSReducer;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.JobStatus;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author FM
 * @Description
 * @create 2021-09-20 0:53
 */
public class HDFSDriver extends Configured implements Tool {

    @Override
    public int run(String[] args) throws Exception {
        Job job=Job.getInstance();
        job.setJarByClass(HDFSDriver.class);
        job.setMapperClass(HDFSMapper.class);
        job.setMapOutputKeyClass(NullWritable.class);
        job.setMapOutputValueClass(Put.class);
        TableMapReduceUtil.initTableReducerJob("fruit", HDFSReducer.class,job);
        FileInputFormat.setInputPaths(job,new Path(args[0]));
        boolean flag = job.waitForCompletion(true);
        return flag?JobStatus.State.SUCCEEDED.getValue():JobStatus.State.FAILED.getValue();
    }

    public static void main(String[] args) throws Exception{
        ToolRunner.run(new HDFSDriver(), args);
    }
}
