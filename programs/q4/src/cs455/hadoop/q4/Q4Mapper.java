package cs455.hadoop.q4;

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
 * Extracts Year from index 1, UniqueCarrier from index 9, and CarrierDelay from index 25
 * Compares UniqueCarrier with carriers.csv table to extract carrier name
 * Emits <CarrierName, (DelayCount,DelayTime)> pairs 
 */
public class Q4Mapper extends Mapper<LongWritable, Text, Text, Text> {

    Map<String, String> carrierData = new HashMap<String, String>(); // Store data with <Code, Description>
    
    @Override
    public void setup(
	Mapper<LongWritable, Text, Text, Text>.Context context)
	throws IOException, InterruptedException {

	if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
	    Path[] localPaths = context.getLocalCacheFiles();
	    BufferedReader reader = new BufferedReader(new FileReader(localPaths[0].toString()));
	    reader.readLine();
	    String carrierRecord = reader.readLine();
	    while (carrierRecord != null) {
		String[] lineData = carrierRecord.split(",").replaceAll("^\"|\"$", "");
		carrierData.put(lineData[0], lineData[1]);
		carrierRecord = reader.readLine();
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
	String uniqueCarrier = record[8];
	String carrierDelay = record[24];
			
	// Check against carriers.csv to determine CarrierName	
	if (!uniqueCarrier.equals("NA") && !uniqueCarrier.equals("UniqueCarrier")) {
	    String carrierName = carrierData.get(uniqueCarrier);
	    if (carrierInfo != null) {
		String delayInfo = "1," + carrierDelay;
		context.write(new Text(carrierName), new Text(delayInfo));		
	    }
	}
	
    }
    
}
