package com.mapreduceremind.wordcount;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-09-02 20:55
 */
public class JobSubmitter {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        // jvm参数 user=root
        System.setProperty("HADOOP_USER_NAME", "root");

        // 找到 hdfs MASTER
        conf.set("fs.defaultFS", "hdfs://hadoop-01:9000");
        // 设置分布式计算框架类型
        conf.set("mapreduce.framework.name", "yarn");
        // 找到yarn Resource manager 主机
        conf.set("yarn.resourcemanager.hostname", "hadoop-04");

        // 如果是windows bash  会出现拼接问题
        //conf.set("mapreduce.app-submission.cross-platform", "true");

        // 分配内存
        conf.set("mapreduce.map.memory.mb", "1024");
        conf.set("mapreduce.reduce.memory.mb", "1024");

        Job job = Job.getInstance(conf);
        //job.setJarByClass(JobSubmitter.class);
        job.setJar("/Users/haoxuanli/Documents/GitHub/MapReduceDemo/target/MapReduceDemo-1.0-SNAPSHOT.jar");
        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.setInputPaths(job, new Path("/input"));

        // 此路径一定为空 否则抛异常
        FileOutputFormat.setOutputPath(job, new Path("/output"));

        job.waitForCompletion(true);
    }
}
