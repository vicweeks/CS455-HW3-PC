package cs455.hadoop.q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/*
 * Mapper: Reads line by line (one line is one record with 29 comma-separated fields), 
 * (*DayOfWeek*) extracts DayOfWeek from index 4, ArrDelay from index 15, and DepDelay from index 16. 
 * Emits <DayOfWeek, meanDelay>> pairs 
 */
public class Q2Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

	/*
	// tokenize into fields.
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
	// extract hour and delays
	if (itr.countTokens() < 29)
	    return;
	
	checkItr(itr); // Year
	checkItr(itr); // Month
	checkItr(itr); // DayOfMonth
	String dayOfWeek = checkItr(itr); // DayOfWeek
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
	*/

	String[] record = value.toString().split(",");

	if (record.length != 29)
	    return;

	String dayOfWeek = record[3];
	String arrDelay = record[14];
	String depDelay = record[15];
	
	if (dayOfWeek.equals("NA")
	    || dayOfWeek.equals("DayOfWeek")
	    || arrDelay.equals("NA")
	    || arrDelay.equals("ArrDelay")
	    || depDelay.equals("NA")
	    || depDelay.equals("DepDelay"))
	    return;
	
	if (dayOfWeek.length() > 1) // 1 (Monday) - 7 (Sunday)
	    return;	  
		
	// calculate mean overall delay
	int arrDelayInt = Integer.parseInt(arrDelay);
	int depDelayInt = Integer.parseInt(depDelay);
	int meanDelay = (arrDelayInt + depDelayInt) / 2;
	
	IntWritable delay = new IntWritable(meanDelay);
	
	context.write(new Text(dayOfWeek), delay);
		
    }

    private String checkItr(StringTokenizer itr) {
	if (itr.hasMoreTokens())
	    return itr.nextToken();
	return null;
    }
    
}
