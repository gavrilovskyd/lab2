package ru.labs.flights;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Partitioner;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReduceSideJoinKey implements Writable {
    private String joinKey;
    private boolean isUnique;

    public ReduceSideJoinKey() {}

    public ReduceSideJoinKey(int key, boolean unique) {
        this.joinKey = key;
        this.isUnique = unique;
    }

    public void write(DataOutput out) throws IOException {
        out.writeInt(joinKey);
        out.writeByte(isUnique ? 0 : 1);
    }

    public void readFields(DataInput in) throws IOException {
        joinKey = in.readInt();
        isUnique = (in.readByte() == 0);
    }

    public static ReduceSideJoinKey read(DataInput in) throws IOException {
        ReduceSideJoinKey k = new ReduceSideJoinKey();
        k.readFields(in);
        return k;
    }

    public static class partitioner<V> extends Partitioner<ReduceSideJoinKey, V> {
        public int getPartition(ReduceSideJoinKey key, V value, int numReduceTasks) {
            return (key.joinKey)
        }
    }
}
