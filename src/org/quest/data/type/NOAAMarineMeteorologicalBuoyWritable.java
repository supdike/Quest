package org.quest.data.type;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.StringWriter;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.quest.util.ConversionUtils;

public class NOAAMarineMeteorologicalBuoyWritable implements Writable {

	private Text year;
	private Text mon;
	private Text day;
	private FloatWritable waveHeight, airTemperature, waterTemperature;
	
	public NOAAMarineMeteorologicalBuoyWritable() {
		this.year = new Text();
		this.mon = new Text();
		this.day = new Text();
		this.waveHeight = new FloatWritable();
		this.airTemperature = new FloatWritable();
		this.waterTemperature = new FloatWritable();
	}
	
	public NOAAMarineMeteorologicalBuoyWritable(Text y, Text m, Text d, FloatWritable waveH, FloatWritable airT, FloatWritable waterT, boolean convertToFahrenheit) {
		this.year = y;
		this.mon = m;
		this.day = d;
		if (convertToFahrenheit) {
			this.waveHeight = new FloatWritable(ConversionUtils.convertCelsiusToFahrenheit(waveH.get()));
			this.airTemperature = new FloatWritable(ConversionUtils.convertCelsiusToFahrenheit(airT.get()));
			this.waterTemperature = new FloatWritable(ConversionUtils.convertCelsiusToFahrenheit(waterT.get()));
		} else {
			this.waveHeight = waveH;
			this.airTemperature = airT;
			this.waterTemperature = waterT;
		}
	}
	
	public void readFields(DataInput in) throws IOException {
		year.readFields(in);
		mon.readFields(in);
		day.readFields(in);
		waveHeight.readFields(in);
		airTemperature.readFields(in);
		waterTemperature.readFields(in);
	}
	
	public void write(DataOutput out) throws IOException {
		year.write(out);
		mon.write(out);
		day.write(out);
		waveHeight.write(out);
		airTemperature.write(out);
		waterTemperature.write(out);
	}

	public Text getFullDate() {
		StringWriter sw = new StringWriter();
		sw.append(this.getYear().toString());
		sw.append("-");
		sw.append(this.getMon().toString());
		sw.append("-");
		sw.append(this.getDay().toString());
		return new Text(sw.toString());
	}

	public Text getYear() {
		return year;
	}

	public void setYear(Text year) {
		this.year = year;
	}

	public Text getMon() {
		return mon;
	}

	public void setMon(Text mon) {
		this.mon = mon;
	}

	public Text getDay() {
		return day;
	}

	public void setDay(Text day) {
		this.day = day;
	}

	public FloatWritable getWaveHeight() {
		return waveHeight;
	}

	public void setWaveHeight(FloatWritable waveHeight) {
		this.waveHeight = waveHeight;
	}

	public FloatWritable getAirTemperature() {
		return airTemperature;
	}

	public void setAirTemperature(FloatWritable airTemperature) {
		this.airTemperature = airTemperature;
	}

	public FloatWritable getWaterTemperature() {
		return waterTemperature;
	}

	public void setWaterTemperature(FloatWritable waterTemperature) {
		this.waterTemperature = waterTemperature;
	}
	
	
}
