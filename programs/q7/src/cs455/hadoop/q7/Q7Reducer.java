package cs455.hadoop.q7;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Reducer: Input to the reducer is the output from the mapper. 
 * It receives <EventType, sum> pairs.
 * Emits sorted sums of each EventType for each state in corresponding file (e.g. COOutput).
 * Emits sorted sums of each EventType for all states in AllStatesOutput.
 * Emits top occuring event for each state in TopPerState.
 */
public class Q7Reducer extends Reducer<Text, IntWritable, Text, IntWritable> {
    private MultipleOutputs mos;
    Map<Text, Integer> weatherDelays;

    String generateFileName(String k) {
	return "/home/Q7Outputs/" + k+"Output";
    }
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	mos = new MultipleOutputs(context);
	super.setup(context);
        weatherDelays = new HashMap<Text, Integer>();
    }
    
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {

	int count = 0;
	
        // calculate total count
        for(IntWritable val : values){
	    count += val.get();
        }
	
	weatherDelays.put(new Text(key), count);
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {

	Map<String, String> topEventPerState = new HashMap<String, String>();
	
	// Delay Count sort and print	
	Map<Text, Integer> sortedCounts =
	    weatherDelays.entrySet().stream()
	    .sorted(Map.Entry.comparingByValue((Integer o1, Integer o2) -> o2.compareTo(o1)))
	    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
				      (e1, e2) -> e1, LinkedHashMap::new));

	for (Text key: sortedCounts.keySet()) {
	    String[] keyArr = key.toString().split(",");
	    String state = keyArr[0];   
	    String eventType = keyArr[1];
	    int eventCount = sortedCounts.get(key);
	    topEventPerState.putIfAbsent(state, eventType + "," + Integer.toString(eventCount));
	    mos.write(new Text(eventType), new IntWritable(eventCount), generateFileName(state));
	}

	// TopPerState sort and print
	TreeMap<String, String> sortedStates = new TreeMap<String, String>(topEventPerState);
	
	for (String key: sortedStates.keySet()) {
	    String[] valArr = topEventPerState.get(key).split(",");
	    String eventType = valArr[0];
	    int eventCount = Integer.parseInt(valArr[1]);
	    mos.write(new Text(key + "    " + eventType), new IntWritable(eventCount), "/home/Q7Outputs/TopPerState");
	}
	
	mos.close();
	super.cleanup(context);    
    }
}
