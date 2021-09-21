package com.jd.hbase;

import com.jd.hbase.util.HBaseUtil;

/**
 * @author FM
 * @Description
 * @create 2021-09-19 0:14
 */
public class TestHbaseApi3 {

    public static void main(String[] args) throws Exception{
        HBaseUtil.makeHBaseConnection();

        HBaseUtil.saveData("benefit:emp","1002","info","name","lisi");

        HBaseUtil.close();


    }

}
