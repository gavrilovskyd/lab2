package ru.labs.flights;

import org.apache.hadoop.io.Writable;

public class ReduceSideJoinKey implements Writable {
    private int joinKey;
    private boolean isUnique;
}
