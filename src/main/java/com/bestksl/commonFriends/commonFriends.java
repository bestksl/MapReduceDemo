package com.bestksl.commonFriends;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;

public class commonFriends {
    static class CommonFriendsMapper extends Mapper<LongWritable, Text, Text, Text> {
        //节省内存
        Text v = new Text();
        Text k = new Text();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] userAndFriends = value.toString().split(":");
            String[] friends = userAndFriends[1].split(",");
            String user = userAndFriends[0];
            v.set(user);
            for (String friend :
                    friends) {
                k.set(friend);
                context.write(k, v);
            }

        }
    }


    static class CommonFriendsReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text commonFriend, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            ArrayList<String> list = new ArrayList<String>();
            for (Text friend :
                    values) {
                list.add(friend.toString());
            }
            Collections.sort(list);
            for (int i = 0; i < list.size() - 1; i++) {
                for (int j = i + 1; j < list.size(); j++) {
                    context.write(new Text(list.get(i) + "-" + list.get(j)), commonFriend);
                }
            }
        }
    }
}