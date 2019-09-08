package com.mapreduceremind.threeSmallFiles2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-09-03 18:42
 */
public class TopNReducer extends Reducer<Text, Text, Text, Text> {
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        StringBuilder sb = new StringBuilder();
        for (Text str : values) {
            String[] elements = str.toString().split("\t");
            sb.append(elements[0] + "-->" + elements[1] + " ");
        }
        context.write(key, new Text(sb.toString()));
    }
}
