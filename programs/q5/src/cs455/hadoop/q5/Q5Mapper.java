package cs455.hadoop.q5;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.HashMap;

/*
 * Mapper: Reads line by line (one line is one record with 29 comma-separated fields), 
 * Extracts FlightYear from index 1, TailNum from index 11, ArrDelay from index 15, and DepDelay from index 16
 * Compares TailNum with plane-data.csv table to extract Manufactured Year
 * Determines age with (flightYear - manuYear) > 20 = Old.
 * Emits <Age, CarrierDelay> pairs 
 */
public class Q5Mapper extends Mapper<LongWritable, Text, Text, DoubleWritable> {

    Map<String, String> planeData = new HashMap<String, String>(); // Store data with <tailNum, manuYear>
    
    @Override
    public void setup(
	Mapper<LongWritable, Text, Text, DoubleWritable>.Context context)
	throws IOException, InterruptedException {

	if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
	    Path[] localPaths = context.getLocalCacheFiles();
	    BufferedReader reader = new BufferedReader(new FileReader(localPaths[0].toString()));
	    String planeRecord = reader.readLine();	    
	    while ((planeRecord = reader.readLine()) != null) {
		String[] lineData = planeRecord.split(",");	       
		if (lineData.length != 9)
		    continue;		
		String tailNum = lineData[0];
		String manuYear = lineData[8];
		if (tailNum.equals("NA") || tailNum.equals("None") || manuYear.equals("NA") || manuYear.equals("None"))
		    continue;		
		planeData.put(tailNum, manuYear);
		
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
	String tailNum = record[10];
        String arrDelay = record[14];
	String depDelay = record[15];

	if (year.equals("NA") || year.equals("Year")
	    || tailNum.equals("NA") || tailNum.equals("TailNum")
	    || arrDelay.equals("NA") || arrDelay.equals("arrDelay")
	    || depDelay.equals("NA") || depDelay.equals("arrDelay"))
	    return;
	
	int flightYear = Integer.parseInt(year);
	double delay = (Integer.parseInt(arrDelay) + Integer.parseInt(depDelay)) / 2.0;
	
	// Check against plane-data.csv to determine ManuYear	
	String manuYear = planeData.get(tailNum);
	if (manuYear != null) {
	    int manuYearInt = Integer.parseInt(manuYear);
	    if (manuYearInt == 0)
		return;
	    String ageStatus = "";	    
	    int age = flightYear - manuYearInt;
	    if (age < 0)
		return;
	    if (age > 20)
		ageStatus = "OLD";
	    else ageStatus = "NEW";
	    context.write(new Text(ageStatus), new DoubleWritable(delay));
	    
	    String ageRange = "";	    
	    if (age < 10)
		ageRange = "0 <= Age < 10";
	    else if (age >= 10 && age < 20)
		ageRange = "10 <= Age < 20";
	    else if (age >= 20 && age < 30)
		ageRange = "20 <= Age < 30";
	    else if (age >= 30 && age < 40)
		ageRange = "30 <= Age < 40";
	    else if (age >= 40 && age < 50)
		ageRange = "40 <= Age < 50";
	    else if (age >= 50)
		ageRange = Integer.toString(age);
	    
	    context.write(new Text(ageRange), new DoubleWritable(delay));
	}
	
    }
    
}
