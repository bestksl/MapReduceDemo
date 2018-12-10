package com.bestksl.page.topn.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Topn {
    public class OrderTopNmapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {

    }

}
