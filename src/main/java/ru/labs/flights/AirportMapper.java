package ru.labs.flights;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, AirportWritable, ReduceSideJoinKey, Text> {
    private static final String[] airportHeader = {"Code", "Description"};

    @Override
    protected void map(LongWritable key, AirportWritable value, Context context) throws IOException, InterruptedException {
        context.write(new ReduceSideJoinKey(new Text(record.get("Code")), true),
                new Text(record.get("Description")));
    }
}
