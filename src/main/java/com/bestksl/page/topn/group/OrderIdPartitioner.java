package com.bestksl.page.topn.group;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Partitioner;

public class OrderIdPartitioner extends Partitioner<OrderBean, NullWritable> {

    @Override
    public int getPartition(OrderBean ob, NullWritable nw, int i) {

        return ob.getOrderId().hashCode() & Integer.MAX_VALUE % i;
    }
}
