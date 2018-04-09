package cs455.hadoop.hw3;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.BufferedReader;
import java.io.FileReader;
import java.util.Map;
import java.util.HashMap;

/*
 * Mapper: Reads line by line (one line is one record with 29 comma-separated fields). 
 * Extracts data needed for Q1-Q6.
 * Q1-Q2: Emits <"HOUR:"(hour), (meanDelay,1)>, <"DAY:"(day), (meanDelay,1)>, <"MONTH:"(month), (meanDelay,1)> pairs.
 * Q3: Emits <year, (airportName, sum)> pairs.
 * Q4: Emits <carrierName, (meanDelay,1)> pairs.
 * Q5: Emits <planeAge, (meanDelay,1)> pairs
 * Q6: Emits <city, weatherDelayOccurrance> pairs
 */
public class HW3Mapper extends Mapper<LongWritable, Text, Text, Text> {

    Map<String, String> airportsData = new HashMap<String, String>();
    Map<String, String> carriersData = new HashMap<String, String>();
    Map<String, String> planeData = new HashMap<String, String>();
    
    @Override
    public void setup(Context context) throws IOException, InterruptedException {
	if (context.getCacheFiles() != null && context.getCacheFiles().length > 0) {
	    Path[] localPaths = context.getLocalCacheFiles();
	    BufferedReader airportsFile = new BufferedReader(new FileReader(localPaths[0].toString()));
	    BufferedReader carriersFile = new BufferedReader(new FileReader(localPaths[1].toString()));
	    BufferedReader planesFile = new BufferedReader(new FileReader(localPaths[2].toString()));
	    	    
	    String airportRecord = airportsFile.readLine();
	    while((airportRecord = airportsFile.readLine()) != null) {
		String[] lineData = airportRecord.split(",");
		String data = lineData[1].replaceAll("^\"|\"$", "") + ","
		    + lineData[2].replaceAll("^\"|\"$", "") + ","
		    + lineData[3].replaceAll("^\"|\"$", "");
		airportsData.put(lineData[0].replaceAll("^\"|\"$", ""), data);		
	    }
	    
	    String carrierRecord = carriersFile.readLine();
	    while((carrierRecord = carriersFile.readLine()) != null) {
		String[] lineData = carrierRecord.split(",");
		carriersData.put(lineData[0].replaceAll("^\"|\"$", ""), lineData[1].replaceAll("^\"|\"$", ""));
	    }

	    String planeRecord = planesFile.readLine();
	    while((planeRecord = planesFile.readLine()) != null) {
		String[] lineData = planeRecord.split(",");
		if (lineData.length != 9)
		    continue;
		String tailNum = lineData[0];
		String manuYear = lineData[8];
		if (tailNum.equals("NA") || tailNum.equals("None") || manuYear.equals("NA") || manuYear.equals("None"))
		    continue;
		planeData.put(tailNum, manuYear);
	    }
	    
	}

	super.setup(context);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] record = value.toString().split(",");

	if (record.length != 29)
	    return;

	// extract all relevant data
	String year = record[0];
	String month = record[1];
	String dayOfWeek = record[3];
	String crsDepTime = record[5];
	String uniqueCarrier = record[8];
	String tailNum = record[10];
	String arrDelay = record[14];
	String depDelay = record[15];
	String origin = record[16];
	String dest = record[17];
	String carrierDelay = record[24];
	String weatherDelay = record[25];

	String meanDelay = "NA";
        if (!arrDelay.equals("ArrDelay") && !depDelay.equals("DepDelay"))
	    meanDelay = getMeanDelay(arrDelay, depDelay);
	
	// get and write Q1 and Q2 output
	if (!meanDelay.equals("NA")) {
	    String hourOutput = getHourDelay(crsDepTime);
	    if (!hourOutput.equals("NA"))
		context.write(new Text("Q1," + hourOutput), new Text(meanDelay + ",1"));
	  
	    String dayOutput = getDayDelay(dayOfWeek);
	    if (!dayOutput.equals("NA")) {
		context.write(new Text("Q1," + dayOutput), new Text(meanDelay + ",1"));
	    }

	    String monthOutput = getMonthDelay(month);
	    if (!monthOutput.equals("NA"))		
		context.write(new Text("Q1," + monthOutput), new Text(meanDelay + ",1"));	   
	}

	
	// get and write Q3 output
	if (!origin.equals("NA") && !origin.equals("Origin")) {
	    String originOutput = getQ3Delay(origin);
	    if (!originOutput.equals("NA"))
		context.write(new Text("Q3," + year), new Text(originOutput));
	}

	if (!dest.equals("NA") && !dest.equals("Dest")) {
	    String destOutput = getQ3Delay(dest);
	    if (!destOutput.equals("NA"))
		context.write(new Text("Q3," + year), new Text(destOutput));
	}	


	// get and write Q4 output
	if (!carrierDelay.equals("NA") && !carrierDelay.equals("CarrierDelay")) {
	    int delay = Integer.parseInt(carrierDelay);
	    if (delay > 0) {
		String carrierName = getQ4Delay(uniqueCarrier);
		if (!carrierName.equals("NA"))
		    context.write(new Text("Q4," + carrierName), new Text(carrierDelay + ",1"));
	    }
	}


	// get and write Q5 output
	if (!year.equals("NA") && !year.equals("Year")
	    && !tailNum.equals("NA") && !tailNum.equals("TailNum") && !meanDelay.equals("NA")) {
	    int flightYear = Integer.parseInt(year);
	    String planeDelay = getQ5Delay(tailNum, flightYear);
	    if (!planeDelay.equals("NA")) {
		String[] planeDelayArr = planeDelay.split(",");
		context.write(new Text("Q5," + planeDelayArr[0]), new Text(meanDelay + ",1"));
		context.write(new Text("Q5," + planeDelayArr[1]), new Text(meanDelay + ",1"));
	    }
	}

	// get and write Q6 output
	if (!origin.equals("NA") && !origin.equals("Origin")
	    && !depDelay.equals("NA") && !depDelay.equals("DepDelay")
	    && !weatherDelay.equals("NA") && !weatherDelay.equals("WeatherDelay")) {
	    String city = getQ6Delay(origin, depDelay, weatherDelay);
	    if (!city.equals("NA")) {
		context.write(new Text("Q6," + city), new Text("1"));
	    }
	}
    }

    private String getMeanDelay(String arrDelay, String depDelay) {
	// calculate mean delay
	int arrDelayInt = 0;
	int depDelayInt = 0;
	
	if (!arrDelay.equals("NA"))
	    arrDelayInt = Integer.parseInt(arrDelay);
	if (!depDelay.equals("NA"))
	    depDelayInt = Integer.parseInt(depDelay);
	if (arrDelayInt <= 0 && depDelayInt <= 0)
	    return "NA"; // no delay
	else if (arrDelayInt <= 0)
	    return Double.toString(depDelayInt * 1.0); // only DepDelay
	else if (depDelayInt <= 0)
	    return Double.toString(arrDelayInt * 1.0); // only ArrDelay

	// if both arr and dep delay are positive, return mean
	double meanDelay = (arrDelayInt + depDelayInt) / 2.0;
	return Double.toString(meanDelay);
    }
    
    private String getHourDelay(String crsDepTime) {
	// write output for hour
	if (!crsDepTime.equals("NA") && !crsDepTime.equals("CRSDepTime")) {
	    String hour = "";
	    int hourInt = Integer.parseInt(crsDepTime);
	    hourInt = hourInt % 24;
	    if (hourInt == 0)
		hour = "00";
	    else if (hourInt > 0 && hourInt < 10)
		hour = "0" + Integer.toString(hourInt);
	    else if (hourInt >= 10)
		hour = Integer.toString(hourInt);
	    hour = "HOUR:" + hour;
	    return hour;
	} else
	    return "NA";	    
    }

    private String getDayDelay(String dayOfWeek) {
	// write output for day	
	if (dayOfWeek.length() == 1) { // 1 (Monday) - 7 (Sunday)
	    dayOfWeek = "DAY:" + dayOfWeek;
	    return dayOfWeek;
	} else
	    return "NA";
    }

    private String getMonthDelay(String month) {
	// write output for month
	if (!month.equals("NA") && !month.equals("Month")) {
	    if (month.length() == 1) // (1-9)
		month = "0" + month;
	    month = "MONTH:" + month;
	    return month;
	} else
	    return "NA";
    }

    private String getQ3Delay(String airportCode) {
	String airportInfo = airportsData.get(airportCode);
	if (airportInfo != null) {
		String[] airportInfoArr = airportInfo.split(",");
		String state = airportInfoArr[2];
		if (!state.equals("AK") && !state.equals("HI")) {
		    String airportName = airportInfoArr[0] + ",1";
		    return airportName;
		}
	}
	return "NA";
    }

    private String getQ4Delay(String uniqueCarrier) {
	if (!uniqueCarrier.equals("NA") && !uniqueCarrier.equals("UniqueCarrier")) {
		String carrierName = carriersData.get(uniqueCarrier);
		if (carrierName != null)		   
		    return carrierName;		
		else
		    return "NA";		
	}
	return "NA";
    }

    private String getQ5Delay(String tailNum, int flightYear) {
	String manuYear = planeData.get(tailNum);
	if (manuYear != null) {
	    int manuYearInt = Integer.parseInt(manuYear);
	    if (manuYearInt == 0)
		return "NA";
	    String ageStatus = "";	    
	    int age = flightYear - manuYearInt;
	    if (age < 0)
		return "NA";
	    if (age > 20)
		ageStatus = "OLD,";
	    else ageStatus = "NEW,";
	    
	    String ageRange = "";	    
	    if (age < 10)
		ageRange = "0 <= Age < 10";
	    else if (age >= 10 && age < 20)
		ageRange = "10 <= Age < 20";
	    else if (age >= 20 && age < 30)
		ageRange = "20 <= Age < 30";
	    else if (age >= 30 && age < 40)
		ageRange = "30 <= Age < 40";
	    else if (age >= 40 && age < 50)
		ageRange = "40 <= Age < 50";
	    else if (age >= 50)
		ageRange = Integer.toString(age);

	    return ageStatus + ageRange;
	}
	return "NA";
    }

    private String getQ6Delay(String origin, String depDelay, String weatherDelay) {
	int depDelayInt = Integer.parseInt(depDelay);
	int weatherDelayInt = Integer.parseInt(weatherDelay);
	if (depDelayInt <= 0 || weatherDelayInt <= 0)
	    return "NA";
	String airportsInfo  = airportsData.get(origin);
	if (airportsInfo != null) {
	    String[] airportsInfoArr = airportsInfo.split(",");
	    String city = airportsInfoArr[1];
	    if (city != null) {
		return city;
	    }
	}
	return "NA";
    }
    
}
