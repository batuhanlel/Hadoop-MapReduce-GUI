package com.lel.bigdatatermproject.fourth;

import java.io.IOException;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class FourthMapperClass extends MapReduceBase implements Mapper<LongWritable, Text, Text, DoubleWritable> {

    @Override
    public void map(LongWritable key, Text value, OutputCollector<Text, DoubleWritable> collector, Reporter reporter) throws IOException {

        // GREEN DATA
        String valueString = value.toString();
        String[] columnData = valueString.split(",");

        String passangerCount = columnData[7];
        double tripDistance = Double.parseDouble(columnData[8]);

        collector.collect(new Text(passangerCount), new DoubleWritable(tripDistance));
    }

}
