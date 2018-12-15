package com.bestksl.skew.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Random;

public class SkewWordcount2 {
    static class WdCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] wordAndNums = value.toString().split("\t");
            k.set(wordAndNums[0].split("\001")[0]);
            context.write(k, new IntWritable(Integer.parseInt(wordAndNums[1])));
        }
    }

    static class WdCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable num :
                    values) {
                count += num.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

    static class WdCountCombiner extends Reducer<Text, IntWritable, Text, IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable i :
                    values) {
                count += i.get();
            }
            context.write(key, new IntWritable(count));
        }
    }

}
