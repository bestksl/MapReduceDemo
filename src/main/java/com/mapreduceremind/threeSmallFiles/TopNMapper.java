package com.mapreduceremind.threeSmallFiles;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-09-03 18:42
 */
public class TopNMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(" ");
        FileSplit fileSplit= (FileSplit) context.getInputSplit();
        String fileName=fileSplit.getPath().getName();
        for (String word : words) {
            context.write(new Text(word+"-"+fileName),new IntWritable(1));
        }
    }
}


class TopNCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        context.write(key, new IntWritable(count));
    }
}
