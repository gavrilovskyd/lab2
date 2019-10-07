package ru.labs.flights;

import org.apache.hadoop.io.Writable;

import java.io.DataOutput;
import java.io.IOException;

public class ReduceSideJoinKey implements Writable {
    private int joinKey;
    private boolean isUnique;

    public void write(DataOutput out) throws IOException {
        out.writeInt(joinKey);
        out.writeInt();
    }
}
