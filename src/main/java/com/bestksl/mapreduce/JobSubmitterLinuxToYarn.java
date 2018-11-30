package com.bestksl.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobSubmitterLinuxToYarn {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // 获得 yarn 客户端对象
        Job job = Job.getInstance(conf);

        job.setJarByClass(JobSubmitterLinuxToYarn.class);

        // 封装所需参数: mapper,reducer, output key type, output value type
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        // 封装 output input 路径
        FileInputFormat.setInputPaths(job, new Path("/wordcount/input"));
        FileOutputFormat.setOutputPath(job, new Path("/wordcount/output"));

        // 设置 reducer 数量
        job.setNumReduceTasks(2);


        // 提交yarn
        boolean res = job.waitForCompletion(true);
        System.exit(res ? 0 : 1);
    }

}