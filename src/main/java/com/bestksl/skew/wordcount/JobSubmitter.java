package com.bestksl.skew.wordcount;

import com.bestksl.page.topn.PageTopnMapper;
import com.bestksl.page.topn.PageTopnReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobSubmitter {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("num", "3");
        Job job = Job.getInstance(conf);

        //各种设置
        job.setJarByClass(JobSubmitter.class);
        job.setNumReduceTasks(3);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        job.setMapperClass(SkewWordcount.WdCountMapper.class);
        job.setReducerClass(SkewWordcount.WdCountReducer.class);
        job.setCombinerClass(SkewWordcount.WdCountCombiner.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\mrdata\\wordcount\\input"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\mrdata\\wordcount\\Skew-output"));

        job.waitForCompletion(true);
    }
}