package cs455.hadoop.q4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Reducer: Input to the reducer is the output from the mapper. 
 * It receives <CarrierName, (DelayCount,DelayTime)> pairs.
 * Sums DelayCount and DelayTime for each carrier
 * Emits ordered from highest to lowest <CarrierName, (DelayCount, meanDelayTime)> 
 * pairs in file DelayCount and in file MeanDelayTime.
 */
public class Q3Reducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs mos;
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	super.setup(context);
	mos = new MultipleOutputs(context);
    }
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> delayCount = new HashMap<String, Integer>();
	Map<String, Integer> meanDelay = new HashMap<String, Integer>();
	
        // calculate total count
        for(Text val : values) {
	    String[] value = val.toString().split(",");
	    if (value.length != 2)
		continue;
	    int count = Integer.parseInt(value[0]);
	    int delayTime = Integer.parseInt(value[1]);
	    if (delayCount.containsKey(key)) {
		int currCount = delayCount.get(key);
		currCount += count;
		delayCount.replace(key, currCount);
	    } else
		delayCount.put(key, count);	    
	    if (meanDelay.containsKey(key)) {
		int sumDelay = meanDelay.get(key);
		currSum += delayTime;
		meanDelay.replace(key, currSum);
	    } else
		meanDelay.put(key, delayTime);
        }

	//TODO
	
	Map<String, Integer> sortedSums =
	    airportSums.entrySet().stream()
	    .sorted(Map.Entry.comparingByValue((Integer o1, Integer o2) -> o2.compareTo(o1)))
	    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
				      (e1, e2) -> e1, LinkedHashMap::new));

	Set<Map.Entry<String, Integer>> output = sortedSums.entrySet();
	
	// write to file named for Corresponding year
	int counter = 0;
	for (Map.Entry<String, Integer> entry : output) {
	    mos.write(new Text(entry.getKey()), new IntWritable(entry.getValue()), generateFileName(key));
	    if (counter >= 9)
		return;
	    else
		counter++;
	}
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
	mos.close();    
	super.cleanup(context);    
    }
}
