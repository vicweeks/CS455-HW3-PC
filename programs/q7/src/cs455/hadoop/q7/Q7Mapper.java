package cs455.hadoop.q7;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.HashMap;

/*
 * Mapper: Reads line by line (one line is one record with 29 comma-separated fields), 
 * Extracts Year from index 1, Month from index 2, DayOfMonth from index 3, CRSDepTime from index 6,
 * Origin from index 17, and WeatherDelay from index 26
 * Consults WeatherData table to extract WeatherType
 * Emits <WeatherType, sum> pairs 
 */
public class Q7Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Map<String, String> airportData = new HashMap<String, String>(); // Store data with <iata, airport_data>
    Map<String, String> weatherData = new HashMap<String, String>(); // Store data with <(State-Year-Month-Day), eventType>
    
    @Override
    public void setup(
	Mapper<LongWritable, Text, Text, IntWritable>.Context context)
	throws IOException, InterruptedException {

	if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
	    Path[] localPaths = context.getLocalCacheFiles();
	    BufferedReader airportsReader = new BufferedReader(new FileReader(localPaths[0].toString()));
	    BufferedReader weatherReader = new BufferedReader(new FileReader(localPaths[1].toString()));
	    // store airportData
	    String airportRecord = airportsReader.readLine();
	    while ((airportRecord = airportsReader.readLine()) != null) {
		String[] lineData = airportRecord.split(",");
		String iata = lineData[0].replaceAll("^\"|\"$", "");
		String state = lineData[3].replaceAll("^\"|\"$", "");
		airportData.put(iata, state);
	    }

	    // store weatherData
	    String weatherRecord;
	    while ((weatherRecord = weatherReader.readLine()) != null) {
		String[] lineData = weatherRecord.split(",");
		String key = lineData[0]+lineData[2]+lineData[3]+lineData[4]+lineData[5];
		String eventType = lineData[6];
		weatherData.put(key, eventType);
	    }
	}

	super.setup(context);
    }
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	// Get input line as String array
	String[] record = value.toString().split(",");

	// Make sure all fields are populated so that record is valid
	if (record.length != 29)
	    return;

	String year = record[0];
	String month = record[1];
	String dayOfMonth = record[2];
	String crsDepTime = record[5];
	String origin = record[16];
	String weatherDelay = record[25];

	// Check if there was a weatherDelay
	if (origin.equals("NA") || origin.equals("Origin")	    
	    || weatherDelay.equals("NA") || weatherDelay.equals("WeatherDelay"))
	    return;

	String hour = "NA";
	if (!crsDepTime.equals("NA") && !crsDepTime.equals("CRSDepTime")) {
	    int hourInt = Integer.parseInt(crsDepTime);
	    hourInt = hourInt % 24;
	    hour = Integer.toString(hourInt);
	}
	
	String state = airportData.get(origin);
	
	if (state != null) {	   
	    String compareKey = state + year + month + dayOfMonth + hour;
	    String eventType = weatherData.get(compareKey);
	    if (eventType != null)
		context.write(new Text(eventType), new IntWritable(1));	    
	}
	
    }
    
}
