package cs455.hadoop.q6;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Reducer: Input to the reducer is the output from the mapper. 
 * It receives <City, WeatherDelay> pairs.
 * Keeps records of Citys and total delays in map
 * Sorts records in cleanup.
 * Emits top 10 cities with the most delays.
 */
public class Q6Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    Map<Text, Integer> cityDelays;

    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	super.setup(context);
        cityDelays = new HashMap<Text, Integer>();
    }
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	int count = 0;
	
        // calculate total count
        for(IntWritable val : values){
	    count += val.get();
        }

	cityDelays.put(new Text(key), count);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {

	// Delay Count sort and print	
	Map<Text, Integer> sortedCounts =
	    cityDelays.entrySet().stream()
	    .sorted(Map.Entry.comparingByValue((Integer o1, Integer o2) -> o2.compareTo(o1)))
	    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
				      (e1, e2) -> e1, LinkedHashMap::new));

	int counter = 0;
	for (Text key: sortedCounts.keySet()) {
	    if (counter++ == 10) {
		super.cleanup(context);
		return;
	    }
	    context.write(key, new IntWritable(sortedCounts.get(key)));
	}
	
	super.cleanup(context);    
    }
}
