package cs455.hadoop.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * Reducer: Input to the reducer is the output from the mapper. 
 * (*DayOfWeek*) It receives <DayOfWeek, Delay> pairs.
 * Sums up numEntries and delays for each DayOfWeek. 
 * Extracts mean by dividing delaySum by numEntries.
 * Emits means as <DayOfWeek, meanDelay> pairs.
 */
public class Q1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //TODO
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
