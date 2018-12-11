package com.bestksl.page.topn.group;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;

public class JobSubmitter {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        //各种设置
        job.setJarByClass(JobSubmitter.class);
        job.setPartitionerClass(OrderIdPartitioner.class);
        job.setMapOutputKeyClass(OrderBean.class);
        job.setMapOutputValueClass(NullWritable.class);
        job.setMapperClass(Topn.OrderTopNmapper.class);
        job.setReducerClass(Topn.OrderTopNReducer.class);
        job.setNumReduceTasks(2);
        job.setOutputKeyClass(OrderBean.class);
        job.setOutputValueClass(NullWritable.class);
        job.setGroupingComparatorClass(OrderTopNGroupcomparator.class);

        FileInputFormat.setInputPaths(job, new Path("E:\\mrdata\\flow\\groupinput"));
        FileOutputFormat.setOutputPath(job, new Path("E:\\mrdata\\flow\\output100"));

        job.waitForCompletion(true);

    }
}
