package cs455.hadoop.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

/*
 * Mapper: Reads line by line (one line is one record with 29 comma-separated fields), 
 * (*DEMO*) extracts Carrier Code from index 9. Emits <UniqueCarrier, 1> pairs 
 */
public class Q1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    //TODO
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        // tokenize into fields.
        StringTokenizer itr = new StringTokenizer(value.toString(), ",");
	// extract UniqueCarrier

	while (itr.hasMoreTokens()) {	

	    if (itr.countTokens() >= 29) {
	    
	itr.nextToken(); // 1: Year
	itr.nextToken(); // 2: Month
	itr.nextToken(); // 3: DayOfMonth
	itr.nextToken(); // 4: DayOfWeek
	itr.nextToken(); // 5: DepTime
	itr.nextToken(); // 6: CRSDepTime
	itr.nextToken(); // 7: ArrTime
	itr.nextToken(); // 8: CRSArrTime
	
	context.write(new Text(itr.nextToken()), new IntWritable(1)); // 9: UniqueCarrier
	
	itr.nextToken(); // 10: FlightNum
	itr.nextToken(); // 11: TailNum
	itr.nextToken(); // 12: ActualElapsedTime
	itr.nextToken(); // 13: CRSElapsedTime
	itr.nextToken(); // 14: AirTime
	itr.nextToken(); // 15: ArrDelay
	itr.nextToken(); // 16: DepDelay
	itr.nextToken(); // 17: Origin
	itr.nextToken(); // 18: Dest
	itr.nextToken(); // 19: Distance
	itr.nextToken(); // 20: TaxiIn
	itr.nextToken(); // 21: TaxiOut
	itr.nextToken(); // 22: Cancelled
	itr.nextToken(); // 23: CancellationCode
	itr.nextToken(); // 24: Diverted
	itr.nextToken(); // 25: CarrierDelay
	itr.nextToken(); // 26: WeatherDelay
	itr.nextToken(); // 27: NASDelay
	itr.nextToken(); // 28: SecurityDelay
	itr.nextToken(); // 29:	LateAircraftDelay

	    }
	    else {
	        return;
	    }
	}
    }
}
