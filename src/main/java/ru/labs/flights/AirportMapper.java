package ru.labs.flights;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, ReduceSideJoinKey, Text> {
    private static final String[] airportHeader = {"Code", "Description"};

    private String CODE_FIELD = "Code";
    private String DESCRIPTION_FIELD = "Description";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        CSVParser parser = CSVParser.parse(value.toString(), CSVFormat.RFC4180.withHeader(airportHeader));
        CSVRecord record = parser.getRecords().get(0);

        context.write(new ReduceSideJoinKey(new Text(record.get(CODE_FIELD)), true),
                new Text(record.get(DESCRIPTION_FIELD)));
    }
}
