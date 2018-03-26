package cs455.hadoop.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/*
 * Reducer: Input to the reducer is the output from the mapper. 
 * It receives <"HOUR:"(hour), delay>, <"DAY:"(day), delay>, and <"MONTH:"(month), delay> pairs.
 * Sums up numEntries and delays for each key. 
 * Extracts mean by dividing delaySum by numEntries.
 * Emits means as <key, meanDelay> pairs.
 */
public class Q1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private MultipleOutputs mos;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	super.setup(context);
	mos = new MultipleOutputs(context);
    }

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
	if (key.toString().charAt(0) == 'H')
	    mos.write(key, new IntWritable(mean), "/home/output-1/HourOutput");
	else if (key.toString().charAt(0) == 'D')
	    mos.write(key, new IntWritable(mean), "/home/output-1/DayOutput");
	else if (key.toString().charAt(0) == 'M')
	    mos.write(key, new IntWritable(mean), "/home/output-1/MonthOutput");
        //context.write(key, new IntWritable(mean));
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
	mos.close();    
	super.cleanup(context);    
    }
}
