package com.bestksl.skew.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobSubmitter2 {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("num", "3");
        Job job = Job.getInstance(conf);

        //各种设置
        job.setJarByClass(JobSubmitter2.class);
        job.setNumReduceTasks(3);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(SkewWordcount2.WdCountMapper.class);
        job.setReducerClass(SkewWordcount2.WdCountReducer.class);
        job.setCombinerClass(SkewWordcount2.WdCountCombiner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\mrdata\\wordcount\\Skew-output"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\mrdata\\wordcount\\Skew-output2"));

        job.waitForCompletion(true);
    }
}