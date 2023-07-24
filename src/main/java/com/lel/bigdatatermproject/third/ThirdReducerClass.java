package com.lel.bigdatatermproject.third;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class ThirdReducerClass extends MapReduceBase implements Reducer<Text, DoubleWritable, Text, Text> {

    @Override
    public void reduce(Text text, Iterator<DoubleWritable> iterator, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {

        List<Double> tipList = new ArrayList<>();

        while (iterator.hasNext()) {
            DoubleWritable value = iterator.next();
            tipList.add(value.get());
        }
        
        Collections.sort(tipList, Collections.reverseOrder());
        

        collector.collect(text, new Text(tipList.get(0).toString()));
    }    
}
