package ru.labs.flights;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, ReduceSideJoinKey, Text> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        CSVParser parser = CSVParser.parse(value.toString(), CSVFormat.RFC4180);
        CSVRecord record = parser.getRecords().get(0); //TODO: catch

        if (Float.parseFloat(record.get(19)) < 1e-6             // Not canceled
                && !record.get(18).isEmpty()                    // Has delay data
                && Float.parseFloat(record.get(18)) > 1e-6) {   // Delay is not 0.00
            context.write(new ReduceSideJoinKey(new Text(record.get(14)), false), new Text(record.get(18)));
        }
    }
}
