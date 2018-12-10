package com.bestksl.page.topn.group;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class OrderTopNGroupcomparator extends WritableComparator {


    public OrderTopNGroupcomparator() {
        super(OrderBean.class, true);
    }

    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        OrderBean o1 = (OrderBean) a;
        OrderBean o2 = (OrderBean) a;
        return o1.compareTo(o2);

    }
}
