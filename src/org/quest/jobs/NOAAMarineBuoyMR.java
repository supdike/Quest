package org.quest.jobs;

import java.io.File;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.log4j.Logger;
import org.quest.data.type.NOAAMarineMeteorologicalBuoyWritable;
import org.quest.mapper.NOAAMarineBuoyMapper;
import org.quest.mapper.NOAAMarineBuoyMapper.NOAA_MARINE_BUOY_COUNTERS;
import org.quest.reducer.NOAAMarineBuoyReducer;

public class NOAAMarineBuoyMR {

	
	static Logger log = Logger.getLogger(NOAAMarineBuoyMR.class);
	
	public static void main(String[] args) throws Exception {
		
		JobConf conf = new JobConf();
		
		String[] otherArgs = new GenericOptionsParser(conf, args)
				.getRemainingArgs();
		if (otherArgs.length != 2) {
			System.err.println("Usage: ..hadoop jar NoaaMR.jar org.quest.jobs.NOAAMarineBuoyMR <in> <out>");
			System.exit(2);
		}
		
		//Load external properties file.
		log.info("Loading local properties file....");
		conf.addResource(new File("/projects/hadoop/scott.xml").toURI().toURL());  //local file - not HDFS
		log.info("Convert temperature readings to Fahrenheit [" + conf.get("convert2fahrenheit") + "]");
		
		Job job = new Job(conf, "Marine Bouy Calculations");
		job.setJarByClass(NOAAMarineBuoyMR.class);
		job.setMapperClass(NOAAMarineBuoyMapper.class);
		// Uncomment this to
		// job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(NOAAMarineBuoyReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NOAAMarineMeteorologicalBuoyWritable.class);
		FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));
		
		boolean success = job.waitForCompletion(true);
		if (success) {
			long bCount = job.getCounters().findCounter(NOAA_MARINE_BUOY_COUNTERS.BUOY_COUNTER).getValue();
			log.info("Number of buoy readings [" + bCount + "]");
		}
		System.exit(success ? 0 : 1);
	}

}
