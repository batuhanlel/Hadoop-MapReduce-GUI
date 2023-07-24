package com.lel.bigdatatermproject.first;

import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FirstMapperClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> collector, Reporter rprtr) throws IOException {
        
        // FHV DATA
        String valueString = value.toString();
        String[] columnData = valueString.split(",");

        String lisancePlate = columnData[0];

        collector.collect(new Text(lisancePlate), new IntWritable(1));
    }

}
