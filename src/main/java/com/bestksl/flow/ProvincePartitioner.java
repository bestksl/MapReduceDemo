package com.bestksl.flow;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

import java.util.HashMap;

public class ProvincePartitioner extends Partitioner<Text, FlowBean> {
    static HashMap<String, Integer> map = new HashMap<>();

    static {
        map.put("135", 0);
        map.put("136", 1);
        map.put("137", 2);
        map.put("138", 3);
        map.put("139", 4);
    }

    @Override
    public int getPartition(Text key, FlowBean f, int i) {
        Integer code = map.get(key.toString().substring(0, 3));
        return code == null ? 5 : code;
    }

}
