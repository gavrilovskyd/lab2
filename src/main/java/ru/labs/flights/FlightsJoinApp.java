package ru.labs.flights;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.partition.HashPartitioner;

public class FlightsJoinApp {
    public static void main(String []args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: FlightsJoinApp <input_flights path> <input_airports path> <output path>");
            System.exit(-1);
        }

        Job job = Job.getInstance();
        job.setJarByClass(FlightsJoinApp.class);
        job.setJobName("FlightsJoinJob");

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, FlightMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, AirportMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.setPartitionerClass(HashPartitioner.class);
        job.setGroupingComparatorClass(WritableComparator.class);
        job.setReducerClass(FlightReduceJoin.class);
        job.setMapOutputKeyClass(ReduceSideJoinKey.class);

        job.setOutputKeyClass(Text.class);
        job.setNumReduceTasks(2);
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
