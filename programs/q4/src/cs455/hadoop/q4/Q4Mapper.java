package cs455.hadoop.q3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.HashMap;

/*
 * Mapper: Reads line by line (one line is one record with 29 comma-separated fields), 
 * Extracts Year from index 1, Origin from index 17, and Dest from index 18
 * Compares Origin and Dest with airports.csv table to extract airport info
 * Emits <year, (airportName, sum)> pairs 
 */
public class Q3Mapper extends Mapper<LongWritable, Text, Text, Text> {

    Map<String, String> airportsData = new HashMap<String, String>(); // Store data with <iata, airport_data>
    
    @Override
    public void setup(
	Mapper<LongWritable, Text, Text, Text>.Context context)
	throws IOException, InterruptedException {

	if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
	    Path[] localPaths = context.getLocalCacheFiles();
	    BufferedReader reader = new BufferedReader(new FileReader(localPaths[0].toString()));
	    reader.readLine();
	    String airportRecord = reader.readLine();
	    while (airportRecord != null) {
		String[] lineData = airportRecord.split(",");
		airportsData.put(lineData[0].replaceAll("^\"|\"$", ""), airportRecord);
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

	String year = record[0];
	String origin = record[16];
	String dest = record[17];
			
	// Check against airports.csv to determine if in Continental U.S.	
	if (!origin.equals("NA") && !origin.equals("Origin")) {
	    String originInfo = airportsData.get(origin);
	    if (originInfo != null) {
		String[] originInfoArr = originInfo.split(",");
		String state = originInfoArr[3].replaceAll("^\"|\"$", "");
		if (!state.equals("AK") && !state.equals("HI")) {
		    String airportInfo = originInfoArr[1].replaceAll("^\"|\"$", "") + ",1";
		    context.write(new Text(year), new Text(airportInfo));
		}
	    } 
	}
	
	if (!dest.equals("NA") && !dest.equals("Dest")) {
	    String destInfo = airportsData.get(dest);
	    if (destInfo != null) {
		String[] destInfoArr = destInfo.split(",");
		String state = destInfoArr[3].replaceAll("^\"|\"$", "");
		if (!state.equals("AK") && !state.equals("HI")) {
		    String airportInfo = destInfoArr[1].replaceAll("^\"|\"$", "") + ",1";
		    context.write(new Text(year), new Text(airportInfo));		
		}
	    }
	}
	
    }
    
}
