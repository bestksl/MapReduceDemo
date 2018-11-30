package com.bestksl.mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobSubmitterWindowsToYarn {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();

        // jvm参数 user=root
        System.setProperty("HADOOP_USER_NAME", "root");

        // 找到 hdfs MASTER
        conf.set("fs.defaultFS", "hdfs://HDP-01:9000");
        // 设置分布式计算框架类型
        conf.set("mapreduce.framework.name", "yarn");
        // 找到yarn Resource manager 主机
        conf.set("yarn.resourcemanager.hostname", "HDP-01");

        // 如果是windows bash  会出现拼接问题
        conf.set("mapreduce.app-submission.cross-platform", "true");

        // 分配内存
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.reduce.memory.mb", "1024");

        // 获得 yarn 客户端对象
        Job job = Job.getInstance(conf);

        // 让 yarn 找到 mapper,reducer, 所在的jar
        //job.setJarByClass(JobSubmitterWindowsToYarn.class); //失败
        job.setJar("d:/a.jar");

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
        Boolean res = job.waitForCompletion(true);
        System.out.println(res);
    }

}