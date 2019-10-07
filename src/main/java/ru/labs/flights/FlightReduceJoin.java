package ru.labs.flights;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FlightReduceJoin extends Reducer<ReduceSideJoinKey, Text, Text, Text> {
}
