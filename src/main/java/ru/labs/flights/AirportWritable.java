package ru.labs.flights;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class AirportWritable implements Writable {
    private static final String[] airportHeader = {"Code", "Description"};
    private Text csvLine;
    private Text code;
    private Text description;

    public AirportWritable() {
        csvLine = new Text();
        code = new Text();
        description = new Text();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        csvLine.readFields(in);

        CSVParser parser = CSVParser.parse(csvLine.toString(), CSVFormat.RFC4180.withHeader(airportHeader));
        CSVRecord record = parser.getRecords().get(0);

        code.set(record.get("Code"));
        description.set(record.get("Description"));
    }

    @Override
    public void write(DataOutput out) throws IOException {
        code.write(out);
        description.write(out);
    }
}
