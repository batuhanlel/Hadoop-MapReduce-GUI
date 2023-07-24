package com.lel.bigdatatermproject.fifth;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FifthMapperClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> collector, Reporter reporter) throws IOException {

        // GREEN DATA
        String valueString = value.toString();
        String[] columnData = valueString.split(",");

        int location = Integer.parseInt(columnData[5]);

        collector.collect(new Text("Top 10 busiest pick up locations : "), new IntWritable(location));
    }

}
