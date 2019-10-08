package ru.labs.flights;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, ReduceSideJoinKey, Text> {
    static final double EPS = 1e-6;
    static final String[] flightHeader = {
            "YEAR","QUARTER","MONTH", "DAY_OF_MONTH","DAY_OF_WEEK","FL_DATE","UNIQUE_CARRIER",
            "AIRLINE_ID","CARRIER","TAIL_NUM","FL_NUM","ORIGIN_AIRPORT_ID","ORIGIN_AIRPORT_SEQ_ID",
            "ORIGIN_CITY_MARKET_ID","DEST_AIRPORT_ID","WHEELS_ON","ARR_TIME","ARR_DELAY","ARR_DELAY_NEW",
            "CANCELLED","CANCELLATION_CODE","AIR_TIME","DISTANCE"
    };

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        CSVParser parser = CSVParser.parse(value.toString(), CSVFormat.RFC4180.withHeader(flightHeader));
        CSVRecord record = parser.getRecords().get(0);

        if (Float.parseFloat(record.get(19)) < 1e-6             // Not canceled
                && !record.get(18).isEmpty()                    // Has delay data
                && Float.parseFloat(record.get(18)) > 1e-6) {   // Delay is not 0.00
            context.write(new ReduceSideJoinKey(new Text(record.get(14)), false), new Text(record.get(18)));
        }
    }
}
