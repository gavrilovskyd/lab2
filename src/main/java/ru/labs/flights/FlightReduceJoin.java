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

        int delayCount = 0;
        float summaryDelay = 0;
        float maxDelay = -1;
        float minDelay = Float.MAX_VALUE;
        while (iter.hasNext()) {
            float delay = Float.parseFloat(iter.next().toString());

            delayCount++;
            summaryDelay += delay;
            maxDelay = Math.max(maxDelay, delay);
            minDelay = Math.min(minDelay, delay);
        }

        if (delayCount != 0) {
            context.write(airportName, new Text((summaryDelay / delayCount)+","+maxDelay+","+minDelay));
        }
    }
}
