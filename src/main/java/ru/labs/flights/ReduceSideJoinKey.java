package ru.labs.flights;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class ReduceSideJoinKey implements WritableComparable<ReduceSideJoinKey> {
    private Text joinKey;
    private int isUnique;

    public ReduceSideJoinKey() {
        joinKey = new Text();
        isUnique = 0;
    }

    public ReduceSideJoinKey(Text key, boolean unique) {
        joinKey = key;
        isUnique = (unique ? 0 : 1);
    }

    @Override
    public void write(DataOutput out) throws IOException {
        joinKey.write(out);
        out.writeByte(isUnique);
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        joinKey.readFields(in);
        isUnique = in.readByte();
    }

    @Override
    public int compareTo(ReduceSideJoinKey k) {
        // this is using in SortComparator inside part.
        int keyResult = joinKey.compareTo(k.joinKey);
        return (keyResult == 0 ? (isUnique - k.isUnique) : keyResult);
    }

    @Override
    public int hashCode() {
        return joinKey.hashCode();
    }

    public static class GroupingComparator extends WritableComparator {
        protected GroupingComparator() {
            super(ReduceSideJoinKey.class, true);
        }

        @Override
        public int compare(WritableComparable key1, WritableComparable key2) {
            ReduceSideJoinKey k1 = (ReduceSideJoinKey)key1;
            ReduceSideJoinKey k2 = (ReduceSideJoinKey)key2;

            // specific version, that uses only first part of key.
            return k1.joinKey.compareTo(k2.joinKey);
        }
    }
}
