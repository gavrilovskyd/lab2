package ru.labs.flights;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class FlightReduceJoin extends Reducer<ReduceSideJoinKey, Text, Text, Text> {
    @Override
    protected void reduce(ReduceSideJoinKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        
    }
}
