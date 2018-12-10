package com.bestksl.page.topn.group;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class Topn {

    public static class OrderTopNmapper extends Mapper<LongWritable, Text, OrderBean, NullWritable> {
        String[] attrs;
        OrderBean o = new OrderBean();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            attrs = value.toString().split(",");
            o.set(attrs[0], attrs[1], attrs[2], Float.parseFloat(attrs[3]), Integer.parseInt(attrs[4]));
            context.write(o, NullWritable.get());
        }
    }

    public static class OrderTopNReducer extends Reducer<OrderBean, NullWritable, OrderBean, NullWritable> {
        @Override
        protected void reduce(OrderBean key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
            int i = 0;
            for (NullWritable nw :
                    values) {
                if (++i == 3) return;
                context.write(key, NullWritable.get());
            }
        }
    }
}
