package cs455.hadoop.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/*
 * Mapper: Reads line by line (one line is one record with 29 comma-separated fields), 
 * (*HOUR*) extracts DepTime from index 5, ArrDelay from index 15, and DepDelay from index 16. 
 * Emits <Hour, meanDelay>> pairs 
 */
public class Q1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // tokenize into fields.
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
	// extract hour and delays
	if (itr.countTokens() < 29)
	    return;
	
	checkItr(itr); // Year
	checkItr(itr); // Month
	checkItr(itr); // DayOfMonth
	checkItr(itr); // DayOfWeek
	String depTime = checkItr(itr); // DepTime
	checkItr(itr); // CRSDepTime
	checkItr(itr); // ArrTime
	checkItr(itr); // CRSArrTime
	checkItr(itr); // UniqueCarrier
	checkItr(itr); // FlightNum
	checkItr(itr); // TailNum
	checkItr(itr); // ActualElapsedTime
	checkItr(itr); // CRSElapsedTime
	checkItr(itr); // AirTime
	String arrDelay = checkItr(itr); // ArrDelay
	String depDelay = checkItr(itr); // DepDelay
	checkItr(itr); // Origin
	checkItr(itr); // Dest
	checkItr(itr); // Distance
	checkItr(itr); // TaxiIn
	checkItr(itr); // TaxiOut
	checkItr(itr); // Canceled
	checkItr(itr); // CancellationCode
	checkItr(itr); // Diverted
	checkItr(itr); // CarrierDelay
	checkItr(itr); // WeatherDelay
	checkItr(itr); // NASDelay
	checkItr(itr); // SecurityDelay
	checkItr(itr); // LateAircraftDelay

	if (depTime.equals("NA")
	    || depTime.equals("DepTime")
	    || arrDelay.equals("NA")
	    || arrDelay.equals("ArrDelay")
	    || depDelay.equals("NA")
	    || depDelay.equals("DepDelay"))
	    return;
	
	String hour = "";
	if (depTime.length() == 2) // midnight
	    hour = "00";	  
	else if (depTime.length() == 3) // (01,09)
	    hour = "0" + depTime.substring(0,1);
	else if (depTime.length() == 4) // (10,23)
	    hour = depTime.substring(0,2);
	else
	    return;
	
	int hourCheck = Integer.parseInt(hour);
	if (hourCheck > 23) // accounts for some values being returned from (24,27) not representing a valid hour
	    return;
		
	// calculate mean overall delay
	int arrDelayInt = Integer.parseInt(arrDelay);
	int depDelayInt = Integer.parseInt(depDelay);
	int meanDelay = (arrDelayInt + depDelayInt) / 2;
	
	IntWritable delay = new IntWritable(meanDelay);
	
	context.write(new Text(hour), delay);
		
    }

    private String checkItr(StringTokenizer itr) {
	if (itr.hasMoreTokens())
	    return itr.nextToken();
	return null;
    }
    
}
