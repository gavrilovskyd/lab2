package ru.labs.flights;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

public class AirportWritable implements Writable {
    private Text csvLine;
}
