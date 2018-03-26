package cs455.hadoop.q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/*
 * Mapper: Reads line by line (one line is one record with 29 comma-separated fields), 
 * (*MONTH*) extracts Month from index 2, ArrDelay from index 15, and DepDelay from index 16. 
 * Emits <Month, meanDelay>> pairs 
 */
public class Q2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // tokenize into fields.
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
	// extract Month and delays
	if (itr.countTokens() < 29)
	    return;
	
	checkItr(itr); // Year
	String month = checkItr(itr); // Month
	checkItr(itr); // DayOfMonth
	checkItr(itr); // DayOfWeek
        checkItr(itr); // DepTime
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

	if (month.equals("NA")
	    || month.equals("Month")
	    || arrDelay.equals("NA")
	    || arrDelay.equals("ArrDelay")
	    || depDelay.equals("NA")
	    || depDelay.equals("DepDelay"))
	    return;

	if (month.length() == 1) // (1-9)
	    month = "0" + month;
	else if (month.length() > 2) // 
	    return;

	// calculate mean overall delay
	int arrDelayInt = Integer.parseInt(arrDelay);
	int depDelayInt = Integer.parseInt(depDelay);
	int meanDelay = (arrDelayInt + depDelayInt) / 2;
	
	IntWritable delay = new IntWritable(meanDelay);
	
	context.write(new Text(month), delay);
		
    }

    private String checkItr(StringTokenizer itr) {
	if (itr.hasMoreTokens())
	    return itr.nextToken();
	return null;
    }
    
}
