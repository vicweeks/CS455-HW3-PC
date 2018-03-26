package cs455.hadoop.q3;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * Reducer: Input to the reducer is the output from the mapper. 
 * It receives <year, (airportName, sum)> pairs.
 * Keeps record of top 10 sums in map <airportName, sum>
 * Emits top 10 sums as <year, (airportName, sum)> pairs.
 */
public class Q3Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
	
        // calculate total count
        for(IntWritable val : values){
	    count += val.get();
        }
	
        context.write(key, new IntWritable(count));
    }
}
