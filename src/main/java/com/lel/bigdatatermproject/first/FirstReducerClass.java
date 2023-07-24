package com.lel.bigdatatermproject.first;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class FirstReducerClass extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, IntWritable> collector, Reporter rprtr) throws IOException {
        int sum = 0;

        while (iterator.hasNext()) {
            IntWritable value = iterator.next();
            sum += value.get();
        }

        collector.collect(text, new IntWritable(sum));
    }

}
