package com.mapreduceremind.threeSmallFiles2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-09-03 09:53
 */
public class JobSubmitterLocal {
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        // jvm参数 user=root
        System.setProperty("HADOOP_USER_NAME", "root");

        // 找到 hdfs MASTER
        //conf.set("fs.defaultFS", "/");
        // 设置分布式计算框架类型
        //conf.set("mapreduce.framework.name", "local");
        //conf.set("yarn.resourcemanager.hostname", "hadoop-04");
        // 找到yarn Resource manager 主机


        Job job = Job.getInstance(conf);
        //job.setJarByClass(JobSubmitter.class);
        job.setOutputKeyClass(Text.class);
        job.setMapperClass(TopNMapper.class);
        job.setOutputValueClass(Text.class);
        //job.setCombinerClass(TopNCombiner.class);
        job.setReducerClass(TopNReducer.class);
        FileInputFormat.setInputPaths(job, new Path("/Users/haoxuanli/Desktop/output"));

        // 此路径一定为空 否则抛异常
        FileOutputFormat.setOutputPath(job, new Path("/Users/haoxuanli/Desktop/output2"));

        job.waitForCompletion(true);
    }
}
