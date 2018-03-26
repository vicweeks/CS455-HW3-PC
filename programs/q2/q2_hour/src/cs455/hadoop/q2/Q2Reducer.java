package cs455.hadoop.q2;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * Reducer: Input to the reducer is the output from the mapper. 
 * (*HOUR*) It receives <Hour, Delay> pairs.
 * Sums up numEntries and delays for each Hour. 
 * Extracts mean by dividing delaySum by numEntries.
 * Emits means as <Hour, meanDelay> pairs.
 */
public class Q2Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int numEntries = 0;
	int delaySum = 0;
	
        // calculate numEntries and delaySum
        for(IntWritable val : values){
	    numEntries++;
	    delaySum += val.get();
        }

	int mean = delaySum / numEntries;
	
        context.write(key, new IntWritable(mean));
    }
}
