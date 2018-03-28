package cs455.hadoop.q4;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/*
 * Reducer: Input to the reducer is the output from the mapper. 
 * It receives <CarrierName, (DelayCount,DelayTime)> pairs.
 * Sums DelayCount and calculates DelayMeans for each carrier
 * Emits <CarrierName, (DelayCount,DelayMean)> pairs
 */
public class Q4Reducer extends Reducer<Text, Text, Text, Text> {
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	int count = 0;
	double sumDelay = 0.0;
	
	for (Text val : values) {
	    String[] value = val.toString().split(",");
	    if (value.length != 2)
		continue;
	    count += Integer.parseInt(value[0]);
	    sumDelay += (double) Integer.parseInt(value[1]);    
	}
	
	double meanToTruncate = sumDelay / count;
	double mean = BigDecimal.valueOf(meanToTruncate).setScale(3, RoundingMode.HALF_UP).doubleValue();
	
	String output = Integer.toString(count) + "," + Double.toString(mean);
	context.write(key, new Text(output));
    }

}
