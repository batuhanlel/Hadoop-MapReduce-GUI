package com.lel.bigdatatermproject.second;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class SecondMapperClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> collector, Reporter reporter) throws IOException {

        // FHVHV Data 
        String valueString = value.toString();
        String[] columnData = valueString.split(",");

        String lisancePlate = columnData[1];
        double tripDistance = Double.parseDouble(columnData[9]);

        collector.collect(new Text(lisancePlate), new DoubleWritable(tripDistance));
    }

}
