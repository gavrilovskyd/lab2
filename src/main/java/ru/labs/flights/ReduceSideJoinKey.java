package ru.labs.flights;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReduceSideJoinKey implements Writable {
    private int joinKey;
    private boolean isUnique;

    public void write(DataOutput out) throws IOException {
        out.writeInt(joinKey);
        out.writeByte(isUnique ? 0 : 1);
    }

    public void readFields(DataInput in) throws IOException {
        joinKey = in.readInt();
        isUnique = (in.readByte() == 0);
    }

    public static ReduceSideJoinKey 
}
