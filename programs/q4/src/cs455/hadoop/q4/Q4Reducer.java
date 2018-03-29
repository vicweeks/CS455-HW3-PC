package cs455.hadoop.q4;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.DoubleWritable;
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
 * Sums DelayCount and calculates DelayMeans for each carrier
 * Stores values in two Maps, sorts Maps and outputs result in cleanup
 * Emits ordered from highest to lowest <CarrierName, (DelayCount, meanDelayTime)> 
 * pairs in file DelayCount and in file MeanDelayTime.
 */
public class Q4Reducer extends Reducer<Text, IntWritable, Text, Text> {
    private MultipleOutputs mos;
    Map<Text, Integer> delayCount;
    Map<Text, Double> meanDelay;
	
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	super.setup(context);
	mos = new MultipleOutputs(context);
	delayCount = new HashMap<Text, Integer>();
	meanDelay = new HashMap<Text, Double>();
    }
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
	
	int count = 0;
        double sumDelay = 0.0;

	for (IntWritable val : values) {
	    count += 1;
	    sumDelay += val.get();
	}
	
	double meanToTruncate = sumDelay / count;
	double mean = BigDecimal.valueOf(meanToTruncate).setScale(3, RoundingMode.HALF_UP).doubleValue();

        delayCount.put(new Text(key), count);	    	
	meanDelay.put(new Text(key), mean);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
	
	// Delay Count sort and print	
	Map<Text, Integer> sortedCounts =
	    delayCount.entrySet().stream()
	    .sorted(Map.Entry.comparingByValue((Integer o1, Integer o2) -> o2.compareTo(o1)))
	    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
				      (e1, e2) -> e1, LinkedHashMap::new));

	// Mean Delay sort and print
	Map<Text, Double> sortedMeans =
	    meanDelay.entrySet().stream()
	    .sorted(Map.Entry.comparingByValue((Double o1, Double o2) -> o2.compareTo(o1)))
	    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
				      (e1, e2) -> e1, LinkedHashMap::new));
	
	for (Text key: sortedCounts.keySet()) {	    
	    String output = Integer.toString(sortedCounts.get(key)) + "    " + Double.toString(sortedMeans.get(key));
	    mos.write(key, new Text(output), "/home/output-4/CarrierDelayCount");
	}


	for (Text key: sortedMeans.keySet()) { 
	    String output = Integer.toString(sortedCounts.get(key)) + "    " + Double.toString(sortedMeans.get(key));
	    mos.write(key, new Text(output), "/home/output-4/CarrierMeanDelay");
	}
	
	mos.close();    
	super.cleanup(context);    
    }
}
