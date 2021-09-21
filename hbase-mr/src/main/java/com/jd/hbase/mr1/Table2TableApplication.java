package com.jd.hbase.mr1;

import com.jd.hbase.mr1.tool.HbaseMapperReduceTool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author FM
 * @Description
 * @create 2021-09-19 23:11
 */
public class Table2TableApplication {

    public static void main(String[] args) throws Exception{
        ToolRunner.run(new HbaseMapperReduceTool(), args);
    }

}
