package org.quest.reducer;

import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.quest.data.type.NOAAMarineMeteorologicalBuoyWritable;

public class NOAAMarineBuoyReducer extends Reducer<Text,NOAAMarineMeteorologicalBuoyWritable,Text,FloatWritable> {
    
	  private Float wvhtTotal = 0.0f;
	  private Float watTmpTotal = 0.0f;
	  private Float airTmpTotal = 0.0f;
	  private int counter = 0;

  public void reduce(Text key, Iterable<NOAAMarineMeteorologicalBuoyWritable> values, 
                     Context context
                     ) throws IOException, InterruptedException {
    
    for (NOAAMarineMeteorologicalBuoyWritable buoyData : values) {
      wvhtTotal += buoyData.getWaveHeight().get();
      watTmpTotal += buoyData.getWaterTemperature().get();
      airTmpTotal += buoyData.getAirTemperature().get();
      counter++;
    }
    String keyString = key.toString();
    
    context.write(new Text("Average Wave Height for " + keyString), new FloatWritable(wvhtTotal/counter));
    context.write(new Text("Average Water Temp for " + keyString), new FloatWritable(watTmpTotal/counter));
    context.write(new Text("Average Air Temp for " + keyString), new FloatWritable(airTmpTotal/counter));
  }
}
