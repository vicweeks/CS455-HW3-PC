package cs455.hadoop.hw3;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Set;
import java.util.stream.Collectors;

/*
 * Reducer: Input to the reducer is the output from the mapper. 
 * Q1-Q2: Emits means as <key, meanDelay    delayCount> pairs into corresponding files (HourOutput, DayOutput, MonthOutput).
 * Q3: Emits as <> pairs
 * Q4:
 * Q5:
 * Q6:
 */
public class HW3Reducer extends Reducer<Text, Text, Text, Text> {
    private MultipleOutputs mos;
    Map<Text, Integer> q4DelayCount;
    Map<Text, Double> q4MeanDelay;
    Map<Text, Integer> q6DelayCount;
    Map<Text, Double> q6MeanDelay;
    
    String generateFileName(String k) {
	return "/home/HW3/Q3Outputs/" + k+"Output";
    }
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	super.setup(context);
	mos = new MultipleOutputs(context);
	q4DelayCount = new HashMap<Text, Integer>();
	q4MeanDelay = new HashMap<Text, Double>();
	q6DelayCount = new HashMap<Text, Integer>();
	q6MeanDelay = new HashMap<Text, Double>();
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	String[] keyInputs = key.toString().split(",");
	String q = keyInputs[0];
	String ident = keyInputs[1];
	
	// Calculate per Question
	if (q.equals("Q1"))
	    reduceQ1(q, ident, values, context);
	else if (q.equals("Q3"))
	    reduceQ3(q, ident, values, context);
	else if (q.equals("Q4"))
	    reduceQ4(q, ident, values, context);
	else if (q.equals("Q5"))
	    reduceQ5(q, ident, values, context);
	else if (q.equals("Q6"))
	    reduceQ6(q, ident, values, context);
    }

    private void reduceQ1(String q, String ident, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	int numEntries = 0;
	double delaySum = 0.0;
	
	// calculate numEntries and delaySum
	for(Text val : values) {
	    String[] valueInputs = val.toString().split(",");
	    delaySum += Double.parseDouble(valueInputs[0]);
	    numEntries += Integer.parseInt(valueInputs[1]);
	}

	double meanToTruncate = delaySum / numEntries;
	double mean = BigDecimal.valueOf(meanToTruncate).setScale(3, RoundingMode.HALF_UP).doubleValue();
	String output = Double.toString(mean);
	
	if (ident.substring(0,4).equals("HOUR")) {
	    mos.write(new Text(ident), new Text(output), "/home/HW3/Q1andQ2Outputs/HourOutput");
	} else if (ident.substring(0,3).equals("DAY")) {
	    String day = ident.substring(4);
	    int index = Integer.parseInt(day);
	    String[] days = {"", "Monday   ", "Tuesday  ", "Wednesday", "Thursday ", "Friday   ", "Saturday ", "Sunday   "};
	    mos.write(new Text(days[index]), new Text(output), "/home/HW3/Q1andQ2Outputs/DayOutput");
	} else if (ident.substring(0,5).equals("MONTH")) {
	    String month = ident.substring(6);
	    int index = Integer.parseInt(month);
	    String[] months = {"", "January  ", "February ", "March    ", "April    ", "May      ", "June      ",
			       "July     ", "August   ", "September", "October  ", "November ", "December "};
	    mos.write(new Text(months[index]), new Text(output), "/home/HW3/Q1andQ2Outputs/MonthOutput");
	}
    }

    private void reduceQ3(String q, String ident, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	String year = ident;
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
	
	Map<String, Integer> sortedSums =
	    airportSums.entrySet().stream()
	    .sorted(Map.Entry.comparingByValue((Integer o1, Integer o2) -> o2.compareTo(o1)))
	    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
				      (e1, e2) -> e1, LinkedHashMap::new));
	    	
	// write to file named for Corresponding year
	int counter = 0;
	for (String airport : sortedSums.keySet()) {
	    mos.write(new Text(airport), new Text(Integer.toString(sortedSums.get(airport))), generateFileName(year));
	    if (counter >= 9)
		return;
	    else
		counter++;
	}
       
    }

    private void reduceQ4(String q, String ident, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	int numEntries = 0;
        double delaySum = 0.0;

	for(Text val : values) {
	    String[] valueInputs = val.toString().split(",");
	    delaySum += Double.parseDouble(valueInputs[0]);
	    numEntries += Integer.parseInt(valueInputs[1]);
	}
	
	double meanToTruncate = delaySum / numEntries;
	double mean = BigDecimal.valueOf(meanToTruncate).setScale(3, RoundingMode.HALF_UP).doubleValue();

        q4DelayCount.put(new Text(ident), numEntries);	    	
	q4MeanDelay.put(new Text(ident), mean);
    }

    private void reduceQ5(String q, String ident, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	int numEntries = 0;
        double delaySum = 0.0;

	for(Text val : values) {
	    String[] valueInputs = val.toString().split(",");
	    delaySum += Double.parseDouble(valueInputs[0]);
	    numEntries += Integer.parseInt(valueInputs[1]);
	}

	double meanToTruncate = delaySum / numEntries;
	double mean = BigDecimal.valueOf(meanToTruncate).setScale(3, RoundingMode.HALF_UP).doubleValue();

	String output = Integer.toString(numEntries) + "    " + Double.toString(mean);
	    
	mos.write(new Text(ident), new Text(output), "/home/HW3/Q5Outputs/PlaneAgeDelay");
    }

    private void reduceQ6(String q, String ident, Iterable<Text> values, Context context) throws IOException, InterruptedException {
	int numEntries = 0;
	double delaySum = 0.0;

	for (Text val : values) {
	    String[] valueInputs = val.toString().split(",");
	    delaySum += Double.parseDouble(valueInputs[0]);
	    numEntries += Integer.parseInt(valueInputs[1]);
	}

	double meanToTruncate = delaySum / numEntries;
	double mean = BigDecimal.valueOf(meanToTruncate).setScale(3, RoundingMode.HALF_UP).doubleValue();

	q6DelayCount.put(new Text(ident), numEntries);
	q6MeanDelay.put(new Text(ident), mean);
    }
    
    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {

	// finish Q4
	// Delay Count sort	
	Map<Text, Integer> q4SortedCounts =
	    q4DelayCount.entrySet().stream()
	    .sorted(Map.Entry.comparingByValue((Integer o1, Integer o2) -> o2.compareTo(o1)))
	    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
				      (e1, e2) -> e1, LinkedHashMap::new));

	// Mean Delay sort
	Map<Text, Double> q4SortedMeans =
	    q4MeanDelay.entrySet().stream()
	    .sorted(Map.Entry.comparingByValue((Double o1, Double o2) -> o2.compareTo(o1)))
	    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
				      (e1, e2) -> e1, LinkedHashMap::new));
	// Delay Count print
	for (Text key: q4SortedCounts.keySet()) {	    
	    String output = Integer.toString(q4SortedCounts.get(key)) + "    " + Double.toString(q4SortedMeans.get(key));
	    mos.write(key, new Text(output), "/home/HW3/Q4Outputs/CarrierDelayCount");
	}

	// Mean Delay print
	for (Text key: q4SortedMeans.keySet()) { 
	    String output = Integer.toString(q4SortedCounts.get(key)) + "    " + Double.toString(q4SortedMeans.get(key));
	    mos.write(key, new Text(output), "/home/HW3/Q4Outputs/CarrierMeanDelay");
	}


	// finish Q6
	// Delay Count sort
	Map<Text, Integer> q6SortedCounts =
	    q6DelayCount.entrySet().stream()
	    .sorted(Map.Entry.comparingByValue((Integer o1, Integer o2) -> o2.compareTo(o1)))
	    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
				      (e1, e2) -> e1, LinkedHashMap::new));

	// Mean Delay sort
	Map<Text, Double> q6SortedMeans =
	    q6MeanDelay.entrySet().stream()
	    .sorted(Map.Entry.comparingByValue((Double o1, Double o2) -> o2.compareTo(o1)))
	    .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue,
				      (e1, e2) -> e1, LinkedHashMap::new));
	
	// Delay Count print
	int counter = 0;
	for (Text key: q6SortedCounts.keySet()) {
	    if (counter++ < 10) {
		String output = Integer.toString(q6SortedCounts.get(key)) + "    " + Double.toString(q6SortedMeans.get(key));
		mos.write(key, new Text(output), "/home/HW3/Q6Outputs/WeatherDelayCount");
	    }
	}
	
	// Mean Delay print
	counter = 0;
	for (Text key: q6SortedMeans.keySet()) {
	    if (counter++ < 10) {
	    	String output = Integer.toString(q6SortedCounts.get(key)) + "    " + Double.toString(q6SortedMeans.get(key));
		mos.write(key, new Text(output), "/home/HW3/Q6Outputs/WeatherMeanDelay");
	    }
	}
	
	mos.close();    
	super.cleanup(context);    
    }
}
