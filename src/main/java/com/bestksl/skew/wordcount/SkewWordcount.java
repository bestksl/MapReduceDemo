package com.bestksl.skew.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Random;

public class SkewWordcount {
    static class WdCountMapper extends Mapper<LongWritable, Text, Text, IntWritable> {
        Random random = new Random();
        Text k = new Text();
        IntWritable v = new IntWritable(1);
        int reduceNum;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            reduceNum = context.getNumReduceTasks();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(" ");
            for (String word :
                    words) {
                int a = random.nextInt(reduceNum);
                k.set(word + "\001" + a);
                context.write(k, v);
            }
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
