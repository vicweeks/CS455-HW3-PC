package cs455.hadoop.q4;

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
 * Extracts UniqueCarrier from index 9 and CarrierDelay from index 25
 * Compares UniqueCarrier with carriers.csv table to extract carrier name
 * Emits <CarrierName, (DelayCount,DelayTime)> pairs 
 */
public class Q4Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    Map<String, String> carrierData = new HashMap<String, String>(); // Store data with <Code, Description>
    
    @Override
    public void setup(
	Mapper<LongWritable, Text, Text, IntWritable>.Context context)
	throws IOException, InterruptedException {

	if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
	    Path[] localPaths = context.getLocalCacheFiles();
	    BufferedReader reader = new BufferedReader(new FileReader(localPaths[0].toString()));
	    reader.readLine();
	    String carrierRecord = reader.readLine();
	    while (carrierRecord != null) {
		String[] lineData = carrierRecord.split(",");
		carrierData.put(lineData[0].replaceAll("^\"|\"$", ""), lineData[1].replaceAll("^\"|\"$", ""));
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

	String uniqueCarrier = record[8];
        String carrierDelay = record[24];
	
	if(!carrierDelay.equals("NA") && !carrierDelay.equals("CarrierDelay")) {   
	    int delay = Integer.parseInt(carrierDelay);
	    if (delay <= 0)
		return;
	    // Check against carriers.csv to determine CarrierName	
	    if (!uniqueCarrier.equals("NA") && !uniqueCarrier.equals("UniqueCarrier")) {
		String carrierName = carrierData.get(uniqueCarrier);
		if (carrierName != null) {		   
		    context.write(new Text(carrierName), new IntWritable(delay));		
		} else
		    context.write(new Text(uniqueCarrier), new IntWritable(delay));
	    } 
	}
    }
    
}
