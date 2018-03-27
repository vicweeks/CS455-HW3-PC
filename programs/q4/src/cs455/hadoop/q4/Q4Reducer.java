package cs455.hadoop.q3;

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
 * It receives <year, (airportName, sum)> pairs.
 * Keeps record of top 10 sums in map <airportName, sum>
 * Emits top 10 sums as <airportName, sum> pairs in files corresponding to the year.
 */
public class Q3Reducer extends Reducer<Text, Text, Text, IntWritable> {
    private MultipleOutputs mos;

    String generateFileName(Text k) {
	return "/home/output-3/" + k.toString()+"Output";
    }
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	super.setup(context);
	mos = new MultipleOutputs(context);
    }
    
    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Map<String, Integer> airportSums = new HashMap<String, Integer>();
	
        // calculate total count
        for(Text val : values){
	    String[] value = val.toString().split(",");
	    if (value.length != 2)
		continue;
	    String airport = value[0];
	    int count = Integer.parseInt(value[1]);
	    if (airportSums.containsKey(airport)) {
		int currSum = airportSums.get(airport);
		currSum += count;
		airportSums.replace(airport, currSum);
	    } else
		airportSums.put(airport, count);
	    /*
	    int currSum = -1;
	    
	    currSum = airportSums.putIfAbsent(airport, count);
	    if (currSum != -1) {
		airportSums.replace(airport, currSum, currSum + count);
	    }
	    */
        }
	
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
