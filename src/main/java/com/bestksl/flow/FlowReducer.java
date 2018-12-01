package com.bestksl.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlowReducer extends Reducer<Text, FlowBean, Text, FlowBean> {
    @Override
    protected void reduce(Text key, Iterable<FlowBean> values, Context context) throws IOException, InterruptedException {
        int upTotal = 0;
        int downTotal = 0;
        for (FlowBean f : values) {
            upTotal += f.getUpFlow();
            downTotal += f.getDownFlow();
        }

        context.write(key, new FlowBean(key.toString(), upTotal, downTotal));
    }
}
