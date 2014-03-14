package org.quest.mapper;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.quest.data.type.NOAAMarineMeteorologicalBuoyWritable;
import org.quest.enums.NOAAMarineMeteorologicalBuoyDataPoints;
import org.quest.text.parsers.NOAAMarineMeteorologicalBuoyDataParser;

public class NOAAMarineBuoyMapper extends Mapper<Object, Text, Text, NOAAMarineMeteorologicalBuoyWritable>{

public enum NOAA_MARINE_BUOY_COUNTERS {BUOY_COUNTER}	
	
public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
	//parse out the date, wh, wt, and at from string
	//populate the NOAAMarineMeteorologicalBuoyWritable object and send to Reducer.
	boolean convTemp = Boolean.parseBoolean(context.getConfiguration().get("convert2fahrenheit"));
	Map<NOAAMarineMeteorologicalBuoyDataPoints,String> map = NOAAMarineMeteorologicalBuoyDataParser.parseRecord(value.toString());
	NOAAMarineMeteorologicalBuoyWritable nmmbw = new NOAAMarineMeteorologicalBuoyWritable(
			new Text(map.get(NOAAMarineMeteorologicalBuoyDataPoints.YEAR)),
			new Text(map.get(NOAAMarineMeteorologicalBuoyDataPoints.MON)),
			new Text(map.get(NOAAMarineMeteorologicalBuoyDataPoints.DAY)),
			new FloatWritable(new Float(map.get(NOAAMarineMeteorologicalBuoyDataPoints.WVHT))), 
			new FloatWritable(new Float(map.get(NOAAMarineMeteorologicalBuoyDataPoints.ATMP))), 
			new FloatWritable(new Float(map.get(NOAAMarineMeteorologicalBuoyDataPoints.WTMP))),
			convTemp);  
	context.getCounter(NOAA_MARINE_BUOY_COUNTERS.BUOY_COUNTER).increment(1);
	context.write(nmmbw.getFullDate(), nmmbw);
}
}
