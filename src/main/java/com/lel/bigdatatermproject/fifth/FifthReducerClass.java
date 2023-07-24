package com.lel.bigdatatermproject.fifth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class FifthReducerClass extends MapReduceBase implements Reducer<Text, IntWritable, Text, Text> {

    @Override
    public void reduce(Text text, Iterator<IntWritable> iterator, OutputCollector<Text, Text> collector, Reporter reporter) throws IOException {

        List<Integer> locationList = new ArrayList<>();

        while (iterator.hasNext()) {
            IntWritable value = iterator.next();
            locationList.add(value.get());
        }

        Map<Integer, Integer> locationCountMap = new HashMap<>();

        for (Integer location : locationList) {
            if (locationCountMap.containsKey(location)) {
                int count = locationCountMap.get(location);
                locationCountMap.put(location, count + 1);
            } else {
                locationCountMap.put(location, 1);
            }
        }

        List<Map.Entry<Integer, Integer>> entryList = new ArrayList<>(locationCountMap.entrySet());
        Collections.sort(entryList, Collections.reverseOrder(Map.Entry.comparingByValue()));
        
        List<Map.Entry<Integer, Integer>> firstTen;
        if (entryList.size() > 10) {
            firstTen = new ArrayList<>(entryList.subList(0, Math.min(entryList.size(), 10)));
        } else {
            firstTen = entryList;
        }

        StringBuilder locations = new StringBuilder("[");
        for (Map.Entry<Integer, Integer> e : firstTen) {
            locations.append(e.getKey().toString()).append(", ");
        }
        locations.replace(locations.lastIndexOf(","), locations.lastIndexOf(",") + 1, "]");
        
        collector.collect(text, new Text(locations.toString()));
    }

}
