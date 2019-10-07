package ru.labs.flights;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

public class AirportMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
}
