package ru.labs.flights;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReduceSideJoinKey implements WritableComparable<ReduceSideJoinKey> {
    private Text joinKey;
    private boolean isUnique;

    public ReduceSideJoinKey() {}

    public ReduceSideJoinKey(Text key, boolean unique) {
        joinKey = key;
        isUnique = unique;
    }

    public void write(DataOutput out) throws IOException {
        joinKey.write(out);
        out.writeByte(isUnique ? 0 : 1);
    }

    public void readFields(DataInput in) throws IOException {
        joinKey.readFields(in);
        isUnique = (in.readByte() == 0);
    }

    public static ReduceSideJoinKey read(DataInput in) throws IOException {
        ReduceSideJoinKey k = new ReduceSideJoinKey();
        k.readFields(in);
        return k;
    }

    public int compareTo(ReduceSideJoinKey k) {
        return joinKey.compareTo(k.joinKey);
    }

    public int hashCode() {
        return joinKey.hashCode();
    }
}
