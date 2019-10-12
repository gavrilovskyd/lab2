package ru.labs.flights;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class FlightMapper extends Mapper<LongWritable, Text, ReduceSideJoinKey, Text> {
    private static final double EPS = 1e-6;
    private static final String[] flightHeader = {
            "YEAR","QUARTER","MONTH", "DAY_OF_MONTH","DAY_OF_WEEK","FL_DATE","UNIQUE_CARRIER",
            "AIRLINE_ID","CARRIER","TAIL_NUM","FL_NUM","ORIGIN_AIRPORT_ID","ORIGIN_AIRPORT_SEQ_ID",
            "ORIGIN_CITY_MARKET_ID","DEST_AIRPORT_ID","WHEELS_ON","ARR_TIME","ARR_DELAY",
            "ARR_DELAY_NEW","CANCELLED","CANCELLATION_CODE","AIR_TIME","DISTANCE"
    };

    private String CANCELED_FIELD = "CANCELLED";
    private String DELAY_FIELD = "ARR_DELAY_NEW";
    private String DEST_AIRPORT_ID_FIELD = "DEST_AIRPORT_ID";

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        CSVParser parser = CSVParser.parse(value.toString(), CSVFormat.RFC4180.withHeader(flightHeader));
        CSVRecord record = parser.getRecords().get(0);

        if (Float.parseFloat(record.get(CANCELED_FIELD)) < EPS                     // Not canceled (not 0.00)
                && !record.get(DELAY_FIELD).isEmpty()                       // Has delay data
                && Float.parseFloat(record.get(DELAY_FIELD)) > EPS) {       // Delay is not 0.00
            context.write(new ReduceSideJoinKey(new Text(record.get(DEST_AIRPORT_ID_FIELD)), false),
                    new Text(record.get(DELAY_FIELD)));
        }
    }
}
