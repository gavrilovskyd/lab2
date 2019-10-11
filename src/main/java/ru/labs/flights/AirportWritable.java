package ru.labs.flights;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.IOException;

public class AirportWritable implements Writable {
    private Text csvLine;

    public AirportWritable() {
        csvLine = new Text();
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        
    }
}
