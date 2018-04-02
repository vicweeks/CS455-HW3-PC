package cs455.hadoop.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.HashMap;

/*
 * Mapper: Reads line by line (one line is one record with 29 comma-separated fields). 
 * 
 */
public class HW3Mapper extends Mapper<LongWritable, Text, Text, Text> {

    Map<String, String> airportsData = new HashMap<String, String>();
    Map<String, String> carriersData = new HashMap<String, String>();
    Map<String, String> planeData = new HashMap<String, String>();
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
	    Path[] localPaths = context.getLocalCacheFiles();
	    BufferedReader airportsFile = new BufferedReader(new FileReader(localPaths[0].toString()));
	    BufferedReader carriersFile = new BufferedReader(new FileReader(localPaths[1].toString()));
	    BufferedReader planesFile = new BufferedReader(new FileReader(localPaths[2].toString()));
	    	    
	    String airportRecord = airportsFile.readLine();
	    while((airportRecord = airportsFile.readLine()) != null) {
		String[] lineData = airportRecord.split(",");
		String data = lineData[1].replaceAll("^\"|\"$", "") + ","
		    + lineData[2].replaceAll("^\"|\"$", "") + ","
		    + lineData[3].replaceAll("^\"|\"$", "");
		airportsData.put(lineData[0].replaceAll("^\"|\"$", ""), data);		
	    }
	    
	    String carrierRecord = carriersFile.readLine();
	    while((carrierRecord = carriersFile.readLine()) != null) {
		String[] lineData = carriersFile.split(",");
		carriersData.put(lineData[0].replaceAll("^\"|\"$", ""), lineData[1].replaceAll("^\"|\"$", ""));
	    }

	    String planeRecord = planesFile.readLine();
	    while((planeRecord = planesFile.readLine()) != null) {
		String[] lineData = planeRecord.split(",");
		if (lineData.length != 9)
		    continue;
		String tailNum = lineData[0];
		String manuYear = lineData[8];
		if (tailNum.equals("NA") || tailNum.equals("None") || manuYear.equals("NA") || manuYear.equals("None"))
		    continue;
		planesData.put(tailNum, manuYear);
	    }
	    
	}

	super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] record = value.toString().split(",");

	if (record.length != 29)
	    return;

	// extract all relevant data
	String year = record[0];
	String month = record[1];
	String dayOfWeek = record[3];
	String crsDepTime = record[5];
	String uniqueCarrier = record[8];
	String tailNum = record[10];
	String arrDelay = record[14];
	String depDelay = record[15];
	String origin = record[16];
	String dest = record[17];
	String carrierDelay = record[24];
	String weatherDelay = record[25];

	// output relevant data for Q1 & Q2
	// <Q1, "crsDepTime,dayOfWeek,month,arrDelay,depDelay">
	String q1 = crsDepTime + "," + dayOfWeek + "," + month + "," + arrDelay + "," + depDelay;
	context.write(new Text("Q1"), new Text(q1));

	// output relevant data for Q3
	// <Q3, "year,origin,dest,airportName">
	if (!origin.equals("NA") && !origins.equals("Origin")) {
	    String originInfo = airportsData.get(origin);
	    if (originInfo != null) {
		String originInfoArr = originInfo.split(",");
		String state = originInfoArr[3];
		if (!state.equals("AK") && !state.equals("HI")) {
		    String q3 = year + "," + origin + "," + dest + "," + originInfoArr[1];
		    context.write(new Text("Q3"), new Text(q3));
		}
	    }
	}

	//TODO Q3 dest
	
    }
    
}
