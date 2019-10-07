package ru.labs.flights;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReduceSideJoinKey implements WritableComparable<ReduceSideJoinKey> {
    private Text joinKey;
    private int isUnique;

    public ReduceSideJoinKey() {
        joinKey = new Text();
    }

    public ReduceSideJoinKey(Text key, boolean unique) {
        joinKey = key;
        isUnique = (unique ? 0 : 1);
    }

    public void write(DataOutput out) throws IOException {
        joinKey.write(out);
        out.writeByte(isUnique);
    }

    public void readFields(DataInput in) throws IOException {
        joinKey.readFields(in);
        isUnique = in.readByte();
    }

    public static ReduceSideJoinKey read(DataInput in) throws IOException {
        ReduceSideJoinKey k = new ReduceSideJoinKey();
        k.readFields(in);
        return k;
    }

    public int compareTo(ReduceSideJoinKey k) {
        int keyResult = joinKey.compareTo(k.joinKey);
        return (keyResult == 0 ? (isUnique - k.isUnique) : keyResult);
    }

    public int hashCode() {
        return joinKey.hashCode();
    }
}
