package ru.labs.flights;

public class FlightsJoinApp {
    public static void main(String []args) throws Exception {
        if (args.length != 2) {
            System.err.println("Usage: FlightsJoinApp <input path> <output path>");
            System.exit(-1);
        }
    }
}
