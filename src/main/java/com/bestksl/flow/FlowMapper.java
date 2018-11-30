package com.bestksl.flow;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlowMapper extends Mapper<LongWritable, Text, Text, FlowBean> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = value.toString();
        String[] fields = line.split("\t");
        String phone = fields[1];
        int upFlow = Integer.parseInt(fields[fields.length - 3]);
        int downFlow = Integer.parseInt(fields[fields.length - 2]);
        // key= phone value= Flowbean
        context.write(new Text(phone), new FlowBean(phone, upFlow, downFlow));
    }
}
