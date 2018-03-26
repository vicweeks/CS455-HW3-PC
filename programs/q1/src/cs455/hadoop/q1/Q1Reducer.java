package cs455.hadoop.q1;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/*
 * Reducer: Input to the reducer is the output from the mapper. 
 * It receives <"HOUR:"(hour), delay>, <"DAY:"(day), delay>, and <"MONTH:"(month), delay> pairs.
 * Sums up numEntries and delays for each key. 
 * Extracts mean by dividing delaySum by numEntries.
 * Emits means as <key, meanDelay> pairs into corresponding files.
 */
public class Q1Reducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
    private MultipleOutputs mos;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	super.setup(context);
	mos = new MultipleOutputs(context);
    }

    @Override
    protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
        int numEntries = 0;
	double delaySum = 0;
	
        // calculate numEntries and delaySum
        for(DoubleWritable val : values){
	    numEntries++;
	    delaySum += val.get();
        }

	double mean = delaySum / numEntries;
	if (key.toString().charAt(0) == 'H')
	    mos.write(key, new DoubleWritable(mean), "/home/output-1/HourOutput");
	else if (key.toString().charAt(0) == 'D') {
	    String day = key.toString().substring(4);
	    int index = Integer.parseInt(day);
	    String[] days = {"", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"};
	    mos.write(new Text(days[index]), new DoubleWritable(mean), "/home/output-1/DayOutput");
	}
	else if (key.toString().charAt(0) == 'M') {
	    String month = key.toString().substring(6);
	    int index = Integer.parseInt(month);
	    String[] months = {"", "January", "February", "March", "April", "May", "June",
			     "July", "August", "September", "October", "November", "December"};
	    mos.write(new Text(months[index]), new DoubleWritable(mean), "/home/output-1/MonthOutput");
	}
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
	mos.close();    
	super.cleanup(context);    
    }
}
