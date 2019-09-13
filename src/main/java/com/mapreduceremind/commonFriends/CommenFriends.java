package com.mapreduceremind.commonFriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-09-09 16:55
 */
public class CommenFriends {

    static class CfMapper extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] elements = value.toString().split(":");
            String user = elements[0];
            String[] friends = elements[1].split(",");
            for (String friend : friends) {
                context.write(new Text(friend), new Text(user));
            }
        }
    }

    static class CfReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

            ArrayList<String> list = new ArrayList<>();
            for (Text value : values) {
                list.add(value.toString());
            }
            for (int i = 0; i < list.size(); i++) {
                for (int j = 1; j < list.size(); j++) {
                    if (list.get(i).toString().compareTo(list.get(j).toString()) >= 0) {
                        continue;
                    }
                    context.write(new Text(list.get(i).toString() + "-" + list.get(j).toString()), new Text(key));
                }
            }
        }
    }

    static class CfMapper2 extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] elements = value.toString().split("\t");
            String userAB = elements[0];
            String commonFriend = elements[1];
            context.write(new Text(userAB), new Text(commonFriend));
        }
    }

    static class CfReducer2 extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> friendsList = new ArrayList<>();
            for (Text friend : values) {
                friendsList.add(friend.toString());
            }
            Collections.sort(friendsList);
            context.write(key, new Text(friendsList.toString()));
        }
    }
}
