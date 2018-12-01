package com.bestksl.page.topn;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

public class PageTopnReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    TreeMap<PageCount, Object> tm = new TreeMap<>(new Comparator<PageCount>() {
        @Override
        public int compare(PageCount p1, PageCount p2) {
            if (p1.getCount() == p2.getCount()) {
                return p1.getPage().compareTo(p2.getPage());
            }
            return p1.getCount() - p2.getCount();
        }
    });

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {


        int count = 0;
        for (IntWritable value :
                values
        ) {
            count += value.get();
        }
        PageCount pc = new PageCount();
        pc.setPageCount(key.toString(), count);
        tm.put(pc, null);
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        Set<Map.Entry<PageCount, Object>> entries = tm.entrySet();
        int flag = 0;
        for (Map.Entry<PageCount, Object> e :
                entries) {
            if (flag >= 5) {
                break;
            }
            context.write(new Text(e.getKey().getPage()), new IntWritable(e.getKey().getCount()));
            flag++;
        }

    }
}
