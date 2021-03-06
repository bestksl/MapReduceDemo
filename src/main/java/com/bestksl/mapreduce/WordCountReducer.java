package com.bestksl.mapreduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        Iterator<IntWritable> it = values.iterator();
        int count = 0;
        while (it.hasNext()) {
            IntWritable iw = it.next();
            count += iw.get();
        }
        context.write(key, new IntWritable(count));
    }
}