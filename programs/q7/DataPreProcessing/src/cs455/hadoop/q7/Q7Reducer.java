package cs455.hadoop.q7;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/*
 * Reducer: Writes new file with preprocessed data
 */
public class Q7Reducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs mos;

      @Override
    public void setup(Context context) throws IOException, InterruptedException {
	super.setup(context);
	mos = new MultipleOutputs(context);       
    }
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {

        for(Text val : values){
	    mos.write(key, val, "/home/data/preprocessed/WeatherData");
        }
		
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
	mos.close();
	super.cleanup(context);
    }
}
