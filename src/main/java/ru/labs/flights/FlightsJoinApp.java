package ru.labs.flights;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.lib.MultipleInputs;
import org.apache.hadoop.mapreduce.Job;

public class FlightsJoinApp {
    public static void main(String []args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: FlightsJoinApp <input path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(FlightsJoinApp.class);
        job.setJobName("FlightsJoinJob");

        MultipleInputs.addInputPath(job, new Path(args[]));
    }
}
