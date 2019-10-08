package ru.labs.flights;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class AirportMapper extends Mapper<LongWritable, Text, ReduceSideJoinKey, Text> {
    private static final String[] flightHeader = {
            "YEAR","QUARTER","MONTH", "DAY_OF_MONTH","DAY_OF_WEEK","FL_DATE","UNIQUE_CARRIER",
            "AIRLINE_ID","CARRIER","TAIL_NUM","FL_NUM","ORIGIN_AIRPORT_ID","ORIGIN_AIRPORT_SEQ_ID",
            "ORIGIN_CITY_MARKET_ID","DEST_AIRPORT_ID","WHEELS_ON","ARR_TIME","ARR_DELAY","ARR_DELAY_NEW",
            "CANCELLED","CANCELLATION_CODE","AIR_TIME","DISTANCE"
    };

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        CSVParser parser = CSVParser.parse(value.toString(), CSVFormat.RFC4180.withHeader("Code", "Description"));
        CSVRecord record = parser.getRecords().get(0);

        context.write(new ReduceSideJoinKey(new Text(record.get("Code")), true),
                new Text(record.get("Description")));
    }
}
