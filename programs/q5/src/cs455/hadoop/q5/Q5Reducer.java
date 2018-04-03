package cs455.hadoop.q5;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;

/*
 * Reducer: Input to the reducer is the output from the mapper. 
 * It receives <Age, CarrierDelay> pairs.
 * Calculates DelayMeans for each Age (Old, New).
 * Emits <Age, meanDelayTime> pairs.
 */
public class Q5Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    
    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
	
	int count = 0;
        double sumDelay = 0.0;

	for (DoubleWritable val : values) {
	    count += 1;
	    sumDelay += val.get();
	}

	String output = key.toString() + "    " + Integer.toString(count);
	
	double meanToTruncate = sumDelay / count;
	double mean = BigDecimal.valueOf(meanToTruncate).setScale(3, RoundingMode.HALF_UP).doubleValue();

        context.write(new Text(output), new DoubleWritable(mean));
	
    }
}
