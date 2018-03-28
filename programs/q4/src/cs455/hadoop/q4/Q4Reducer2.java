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
import java.math.BigDecimal;
import java.math.RoundingMode;

/*
 * Reducer: Input to the reducer is the output from the mapper. 
 * It receives <CarrierName, (DelayCount,DelayTime)> pairs.
 * Sums DelayCount and DelayTime for each carrier
 * Emits ordered from highest to lowest <CarrierName, (DelayCount, meanDelayTime)> 
 * pairs in file DelayCount and in file MeanDelayTime.
 */
public class Q4Reducer2 extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs mos;
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	super.setup(context);
	mos = new MultipleOutputs(context);
    }
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	String carrierName = key.toString();

	/*
	Map<String, Integer> delayCount = new HashMap<String, Integer>();
	Map<String, Double> meanDelay = new HashMap<String, Double>();
	
        // calculate total count
        for(Text val : values) {
	    String[] value = val.toString().split(",");
	    if (value.length != 2)
		continue;
	    int count = Integer.parseInt(value[0]);
	    double delayTime = (double) Integer.parseInt(value[1]);
	    if (delayCount.containsKey(carrierName)) {
		int currCount = delayCount.get(carrierName);
		currCount += count;
		delayCount.replace(carrierName, currCount);
	    } else
		delayCount.put(carrierName, count);	    
	    if (meanDelay.containsKey(carrierName)) {
		double sumDelay = meanDelay.get(carrierName);
		currSum += delayTime;
		meanDelay.replace(carrierName, currSum);
	    } else
		meanDelay.put(carrierName, delayTime);
        }

	// Calculate mean delay
	Set<Map.Entry<String, Double>> delaySums = meanDelay.entrySet();
	for (Map.Entry<String, Double> entry : delaySums) {
	    double meanToTruncate = entry.getValue() / delayCount.get(entry.getKey());
	    double mean = BigDecimal.valueOf(meanToTruncate).setScale(3, RoundingMode.HALF_UP).doubleValue();
	    meanDelay.replace(entry.getKey(), mean);
	}
	
	// Delay Count sort and print
	
	Map<String, Integer> sortedCounts =
	    delayCount.entrySet().stream()
	    .sorted(Map.Entry.comparingByValue((Integer o1, Integer o2) -> o2.compareTo(o1)))
	    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
				      (e1, e2) -> e1, LinkedHashMap::new));

	Set<Map.Entry<String, Integer>> outputCounts = sortedCounts.entrySet();
	
	// write to file 
        for (Map.Entry<String, Integer> entry : outputCounts) {
	    String output = entry.getValue().toString() + "    " + meanDelay.get(entry.getKey()).toString();
	    mos.write(new Text(entry.getKey()), new Text(output), "/home/output-4/CarrierDelayCount");
	}

	// Mean Delay sort and print
	
	Map<String, Double> sortedMeans =
	    meanDelay.entrySet().stream()
	    .sorted(Map.Entry.comparingByValue((Double o1, Double o2) -> o2.compareTo(o1)))
	    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
				      (e1, e2) -> e1, LinkedHashMap::new));

	Set<Map.Entry<String, Double>> outputMeans = sortedMeans.entrySet();
	
	// write to file 
        for (Map.Entry<String, Double> entry : outputMeans) {
	    String output = entry.getValue().toString() + "    " + sortedCounts.get(entry.getKey()).toString();
	    mos.write(new Text(entry.getKey()), new Text(output), "/home/output-4/CarrierMeanDelay");
	}
	*/
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
	mos.close();    
	super.cleanup(context);    
    }
}
