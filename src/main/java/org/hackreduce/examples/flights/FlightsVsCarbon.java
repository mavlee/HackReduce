package org.hackreduce.examples.flights;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.hackreduce.examples.flights.FlightFreqCounter;

/**
 * This MapReduce job will count the total number of Flight records in the data dump.
 *
 */
public class FlightsVsCarbon {
  
	public static void main(String[] args) throws Exception {
		int result = ToolRunner.run(new Configuration(), new FlightFreqCounter(), args);
		System.exit(result);
	}
}
