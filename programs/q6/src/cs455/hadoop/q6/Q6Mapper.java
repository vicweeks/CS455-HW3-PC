package cs455.hadoop.q6;

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
 * Extracts Origin from index 17, and WeatherDelay from index 26
 * Compares Origin  with airports.csv table to extract City
 * Emits <City, WeatherDelay> pairs 
 */
public class Q6Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Map<String, String> airportsData = new HashMap<String, String>(); // Store data with <iata, airport_data>
    
    @Override
    public void setup(
	Mapper<LongWritable, Text, Text, IntWritable>.Context context)
	throws IOException, InterruptedException {

	if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
	    Path[] localPaths = context.getLocalCacheFiles();
	    BufferedReader reader = new BufferedReader(new FileReader(localPaths[0].toString()));
	    reader.readLine();
	    String airportRecord = reader.readLine();
	    while (airportRecord != null) {
		String[] lineData = airportRecord.split(",");
		String iata = lineData[0].replaceAll("^\"|\"$", "");
		String city = lineData[2].replaceAll("^\"|\"$", "");
		airportsData.put(iata, city);
		airportRecord = reader.readLine();
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

	String origin = record[16];
	String weatherDelay = record[25];

	// Check if there was a weatherDelay
	if (origin.equals("NA") || origin.equals("Origin")
	    || weatherDelay.equals("NA") || weatherDelay.equals("WeatherDelay"))
	    return;
			
	String city = airportsData.get(origin);
	if (city != null) {		    
	    //int delay = Integer.parseInt(weatherDelay);	    
	    context.write(new Text(city), new IntWritable(1));	    
	} 	
    }
    
}
