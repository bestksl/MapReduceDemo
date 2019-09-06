package com.mapreduceremind.topN.task2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;

/**
 * @author HaoxuanLi  Github:bestksl
 * @version created date：2019-09-03 18:42
 */
public class TopNReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private TreeMap<PageCount, Object> map = new TreeMap<>((o1, o2) -> {
        if (o2.getCount() == o1.getCount()) {
            return o2.getPage().compareTo(o1.getPage());
        } else {
            return o2.getCount() - o1.getCount();
        }
    });


    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        for (IntWritable value : values) {
            count += value.get();
        }
        PageCount pageCount = new PageCount();
        pageCount.set(key.toString(), count);
        map.put(pageCount, null);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        int i = 0;
        for (PageCount pageCount : map.keySet()) {
            if (i >= 5) {
                return;
            }
            context.write(new Text(pageCount.getPage()), new IntWritable(pageCount.getCount()));
            i++;
        }
        super.cleanup(context);
    }
}
