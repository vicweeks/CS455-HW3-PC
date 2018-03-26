package cs455.hadoop.q1;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/*
 * Mapper: Reads line by line (one line is one record with 29 comma-separated fields), 
 * Extracts DepTime from index 5, DayOfWeek from index 4, and Month from index 2. 
 * Extracts ArrDelay from index 15 and DepDelay from index 16. 
 * Emits <"HOUR:"(hour), meanDelay>, <"DAY:"(day), meanDelay>, <"MONTH:"(month), meanDelay> pairs.
 */
public class Q1Mapper extends Mapper<LongWritable, Text, Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] record = value.toString().split(",");

	if (record.length != 29)
	    return;

	String depTime = record[4];
	String dayOfWeek = record[3];
	String month = record[1];
	String arrDelay = record[14];
	String depDelay = record[15];

	int arrDelayInt = 0;
	int depDelayInt = 0;
	
	if (!arrDelay.equals("NA") && !arrDelay.equals("ArrDelay"))
	    arrDelayInt = Integer.parseInt(arrDelay);
	if (!depDelay.equals("NA") && !depDelay.equals("DepDelay"))
	    depDelayInt = Integer.parseInt(depDelay);
	    
	// calculate mean overall delay
	int meanDelay = (arrDelayInt + depDelayInt) / 2;
	IntWritable delay = new IntWritable(meanDelay);
	
	// write output for hour
	if (!depTime.equals("NA") && !depTime.equals("DepTime")) {
	    String hour = "24"; // Allows parsing as int
	    if (depTime.length() == 2) // midnight
		hour = "00";
	    else if (depTime.length() == 3) // (01,09)
		hour = "0" + depTime.substring(0,1);
	    else if (depTime.length() == 4) // (10,23)
		hour = depTime.substring(0,2);
	    int hourInt = Integer.parseInt(hour);
	    hourInt = hourInt % 24;
	    if (hourInt == 0)
		hour = "00";
	    else if (hourInt > 0 && hourInt < 10)
		hour = "0" + Integer.toString(hourInt);
	    else if (hourInt >= 10)
		hour = Integer.toString(hourInt);
	    hour = "HOUR:" + hour;
	    context.write(new Text(hour), delay);
	}

	// write output for day	
	if (dayOfWeek.length() == 1) { // 1 (Monday) - 7 (Sunday)
	    dayOfWeek = "DAY:" + dayOfWeek;
	    context.write(new Text(dayOfWeek), delay);
	}

	// write output for month
	if (!month.equals("NA") && !month.equals("Month")) {
	    if (month.length() == 1) // (1-9)
		month = "0" + month;
	    month = "MONTH:" + month;
	    context.write(new Text(month), delay);
	}
		
    }
    
}
