package com.lel.bigdatatermproject.third;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class ThirdMapperClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> collector, Reporter reporter) throws IOException {

        // FHVHV DATA
        String valueString = value.toString();
        String[] columnData = valueString.split(",");

        double tip = Double.parseDouble(columnData[17]);

        collector.collect(new Text("tip amount"), new DoubleWritable(tip));
    }

}
