package cs455.hadoop.q7;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/*
 * Mapper: Reads line by line,
 * Extracts State, Year, Month, Day, Hour, Event_Type 
 */
public class Q7Mapper extends Mapper<LongWritable, Text, Text, Text> {
    
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	// Get input line as String array
	String[] record = value.toString().split(",");

	String state = record[8].replaceAll("^\"|\"$", "");
	String year = record[10];
	String month = record[11].replaceAll("^\"|\"$", "");
	String eventType = record[12];
	String date_Time = record[17];

	// convert State to abbreviations
	String stateAbv = getStateAbv(state);

	// convert Month to number
	String monthAbv = getMonthAbv(month);
	
	// extract Day and Hour from date_Time
	String day = "NA";
	String hour = "NA";

	if (date_Time.length() > 13) {
	    day = date_Time.substring(1,3);
	    hour = date_Time.substring(11,13);
	    if (day.charAt(0) == '0')
		day = day.substring(1);
	    if (hour.charAt(0) == '0')
		hour = hour.substring(1);
	}	
	
	String output = "," + year + "," + monthAbv + "," + day + "," + hour + "," + eventType;
	    
	context.write(new Text(stateAbv + ","), new Text(output));	    
       	
    }

    private String getMonthAbv(String month) {
	switch (month.toLowerCase()) {
	case "january": return "1";
	case "february": return "2";
	case "march": return "3";
	case "april": return "4";
	case "may": return "5";
	case "june": return "6";
	case "july": return "7";
	case "august": return "8";
	case "september": return "9";
	case "october": return "10";
	case "november": return	"11";
	case "december": return "12";
	default: return "NA";
	}
    }
    
    private String getStateAbv(String state) {
	switch (state.toLowerCase()) {
	case "alabama": return "AL";
	case "alaska": return "AK";
	case "arizona": return "AZ";
	case "arkansas": return "AR";
	case "california": return "CA";
	case "colorado": return "CO";
	case "connecticut": return "CT";
	case "delaware": return "DE";
	case "florida": return "FL";
	case "georgia": return "GA";
	case "hawaii": return "HI";
	case "idaho": return "ID";
	case "illinois": return "IL";
	case "indiana": return "IN";
	case "iowa": return "IA";
	case "kansas": return "KS";
	case "kentucky": return "KY";
	case "louisiana": return "LA";
	case "maine": return "ME";
	case "maryland": return "MD";
	case "massachusetts": return "MA";
	case "michigan": return "MI";
	case "minnesota": return "MN";
	case "mississippi": return "MS";
	case "missouri": return "MO";
	case "montana": return "MT";
	case "nebraska": return "NE";
	case "nevada": return "NV";
	case "new hampshire": return "NH";
	case "new jersey": return "NJ";
	case "new mexico": return "NM";
	case "new york": return "NY";
	case "north carolina": return "NC";
	case "north dakota": return "ND";
	case "ohio": return "OH";
	case "oklahoma": return "OK";
	case "oregon": return "OR";
	case "pennsylvania": return "PA";
	case "rhode island": return "RI";
	case "south carolina": return "SC";
	case "south dakota": return "SD";
	case "tennessee": return "TN";
	case "texas": return "TX";
	case "utah": return "UT";
	case "vermont": return "VT";
	case "virginia": return "VA";
	case "washington": return "WA";
	case "west virginia": return "WV";
	case "wisconsin": return "WI";
	case "wyoming": return "WY";
		    
	default: return "NA";
	}
    }
    
}
