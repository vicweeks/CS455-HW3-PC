package cs455.hadoop.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/*
 * Reducer: Input to the reducer is the output from the mapper. 
 * (*DEMO*) It receives UniqueCarrier, list<count> pairs.
 * (*DEMO*) Sums up individual counts per given UniqueCarrier. Emits <UniqueCarrier, total count> pairs.
 */
public class Q1Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    //TODO
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int count = 0;
        // calculate the total count
        for(IntWritable val : values){
            count += val.get();
        }
        context.write(key, new IntWritable(count));
    }
}
