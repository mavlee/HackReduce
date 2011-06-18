package org.hackreduce.examples.flights;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.hackreduce.mappers.ModelMapper;
import org.hackreduce.mappers.FlightMapper;
import org.hackreduce.models.FlightRecord;

import java.util.Set;
import java.util.HashSet;

/**
 * This MapReduce job will count the frequency of flights between airports 
 *
 */
public class FlightFreqCounter extends Configured implements Tool {

	public enum FlightFreqCounterCount {
		UNIQUE_KEYS
	}
  
  public enum Count {
		TOTAL_RECORDS
	}

	public static class FlightFreqMapper extends FlightMapper<Text, LongWritable> {

		@Override
		protected void map(FlightRecord record, Context context) throws IOException,
				InterruptedException {

			context.getCounter(Count.TOTAL_RECORDS).increment(1);

      String keystring;
      if (record.getOrigin().compareTo(record.getDestination()) < 0) {
        keystring = record.getOrigin() + " " + record.getDestination();
      }
      else {
        keystring = record.getDestination() + " " + record.getOrigin();
      }

			context.write(new Text(keystring), new LongWritable(record.getDepartureTime().getTime()));
		}

	}


	public static class FlightFreqCounterReducer extends Reducer<Text, LongWritable, Text, LongWritable> {

		@Override
		protected void reduce(Text key, Iterable<LongWritable> values, Context context) throws IOException, InterruptedException {
			context.getCounter(FlightFreqCounterCount.UNIQUE_KEYS).increment(1);

      Set <LongWritable> departureTimes = new HashSet <LongWritable> ();

			for (LongWritable value : values) {
        departureTimes.add(value);
			}

			context.write(key, new LongWritable(departureTimes.size()));
		}

	}

	public Class<? extends ModelMapper<?,?,?,?,?>> getMapper() {
    return FlightFreqMapper.class;
  }

	public void configureJob(Job job) {
    FlightFreqMapper.configureJob(job);
  }

	@Override
	public int run(String[] args) throws Exception {
		Configuration conf = getConf();

        if (args.length != 2) {
        	System.err.println("Usage: " + getClass().getName() + " <input> <output>");
        	System.exit(2);
        }

        // Creating the MapReduce job (configuration) object
        Job job = new Job(conf);
        job.setJarByClass(getClass());
        job.setJobName(getClass().getName());

        // Tell the job which Mapper and Reducer to use (classes defined above)
        job.setMapperClass(getMapper());
		job.setReducerClass(FlightFreqCounterReducer.class);

        configureJob(job);

		// This is what the Mapper will be outputting to the Reducer
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(LongWritable.class);

		// This is what the Reducer will be outputting
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);

		// Setting the input folder of the job 
		FileInputFormat.addInputPath(job, new Path(args[0]));

		// Preparing the output folder by first deleting it if it exists
        Path output = new Path(args[1]);
        FileSystem.get(conf).delete(output, true);
	    FileOutputFormat.setOutputPath(job, output);

		return job.waitForCompletion(true) ? 0 : 1;
	}
}
