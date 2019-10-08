package ru.labs.flights;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, ReduceSideJoinKey, Text> {
    static final CSVFormat format = CSVFormat.RFC4180.withHeader("Code", "");

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // TODO: read csv correct
        CSVParser parser = CSVParser.parse(value.toString(), CSVFormat.RFC4180);
        CSVRecord record = parser.getRecords().get(0); //TODO: catch

        context.write(new ReduceSideJoinKey(new Text(record.get(0)), true), new Text(record.get(1)));
    }
}
