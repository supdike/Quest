package org.quest.text.parsers;

import java.util.HashMap;
import java.util.Map;

import org.quest.enums.NOAAMarineMeteorologicalBuoyDataPoints;

public class NOAAMarineMeteorologicalBuoyDataParser {

	public static Map<NOAAMarineMeteorologicalBuoyDataPoints,String> parseRecord(String text) {
		
		Map<NOAAMarineMeteorologicalBuoyDataPoints,String> valMap = new HashMap<NOAAMarineMeteorologicalBuoyDataPoints,String>();
		String[] values = text.split("[ ]+");
		valMap.put(NOAAMarineMeteorologicalBuoyDataPoints.YEAR, values[0]);
		valMap.put(NOAAMarineMeteorologicalBuoyDataPoints.MON, values[1]);
		valMap.put(NOAAMarineMeteorologicalBuoyDataPoints.DAY, values[2]);
		valMap.put(NOAAMarineMeteorologicalBuoyDataPoints.WSPD, values[6]);
		valMap.put(NOAAMarineMeteorologicalBuoyDataPoints.WVHT, values[8]);
		valMap.put(NOAAMarineMeteorologicalBuoyDataPoints.ATMP, values[13]);
		valMap.put(NOAAMarineMeteorologicalBuoyDataPoints.WTMP, values[14]);
		return valMap;
	}
}
