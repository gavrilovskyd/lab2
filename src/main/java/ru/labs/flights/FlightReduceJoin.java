package ru.labs.flights;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Iterator;

public class FlightReduceJoin extends Reducer<ReduceSideJoinKey, Text, Text, Text> {
    @Override
    protected void reduce(ReduceSideJoinKey key, Iterable<Text> values, Context context)
            throws IOException, InterruptedException {
        Iterator<Text> iter = values.iterator();
        Text airportName = new Text(iter.next());

        while (iter.hasNext()) {
            
        }
    }
}
