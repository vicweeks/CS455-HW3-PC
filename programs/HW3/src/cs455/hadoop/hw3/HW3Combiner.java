package cs455.hadoop.hw3;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.Map;
import java.util.HashMap;
import java.math.BigDecimal;
import java.math.RoundingMode;

/*
 * Combiner 
 * Q1-Q2: Combines into <key, "sumDelay, count"> pairs
 * Q3: Combines into <key, "airportName, count"> pairs
 * Q4: Combines into <key, "sumDelay, count"> pairs
 * Q5: Combines into <key, "sumDelay, count"> pairs
 * Q6:
 */
public class HW3Combiner extends Reducer<Text, Text, Text, Text> {

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	String[] keyInputs = key.toString().split(",");
	String q = keyInputs[0];
	
	// Calculate per Question
	if (q.equals("Q1"))
	    reduceQ1(key, values, context);
	else if (q.equals("Q3"))
	    reduceQ3(key, values, context);
	else if (q.equals("Q4"))
	    reduceQ1(key, values, context);
	else if (q.equals("Q5"))
	    reduceQ1(key, values, context);
    }

    private void reduceQ1(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	int numEntries = 0;
	double delaySum = 0.0;
	
	// calculate numEntries and delaySum
	for(Text val : values) {
	    String[] valueInputs = val.toString().split(",");
	    delaySum += Double.parseDouble(valueInputs[0]);
	    numEntries += Integer.parseInt(valueInputs[1]);
	}

	double meanToTruncate = delaySum / numEntries;
	double mean = BigDecimal.valueOf(meanToTruncate).setScale(5, RoundingMode.HALF_UP).doubleValue();
	String output = Double.toString(delaySum) + "," + Integer.toString(numEntries);
	context.write(key, new Text(output));
	
    }

    private void reduceQ3(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
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
	}
	
	for (String airport : airportSums.keySet()) {
	    context.write(key, new Text(airport + "," + airportSums.get(airport)));
	}
       
    }
     
}
