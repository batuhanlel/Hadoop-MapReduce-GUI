package com.lel.bigdatatermproject.second;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class SecondReducerClass extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, DoubleWritable> {

    @Override
    public void reduce(Text text, Iterator<DoubleWritable> iterator, OutputCollector<Text, DoubleWritable> collector, Reporter reporter) throws IOException {
        double sum = 0;
        int count = 0;

        while (iterator.hasNext()) {
            DoubleWritable value = iterator.next();
            sum += value.get();
            count++;
        }

        double avg = sum / count;

        collector.collect(text, new DoubleWritable(avg));
    }

}
