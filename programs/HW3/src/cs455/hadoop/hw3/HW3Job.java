package cs455.hadoop.hw3;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/*
 * This is the main class. Hadoop will invoke the main method of this class.
 */
public class HW3Job {
 
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf, "HW3");
            // Current class.
            job.setJarByClass(HW3Job.class);
	    // Add supplementary datasets to cache
	    job.addCacheFile(new Path("/data/supplementary/airports.csv").toUri());
	    job.addCacheFile(new Path("/data/supplementary/carriers.csv").toUri());
	    job.addCacheFile(new Path("/data/supplementary/plane-data.csv").toUri());
	    // Mapper
            job.setMapperClass(HW3Mapper.class);
	    // Combiner	    
            job.setCombinerClass(HW3Combiner.class);
	    // Reducer
            job.setReducerClass(HW3Reducer.class);
	    //job.setNumReduceTasks(10);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(Text.class);
            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path("/data/main"));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path("/home/HW3"));	    
            // Block until the job is completed.
            System.exit(job.waitForCompletion(true) ? 0 : 1);
        } catch (IOException e) {
            System.err.println(e.getMessage());
        } catch (InterruptedException e) {
            System.err.println(e.getMessage());
        } catch (ClassNotFoundException e) {
            System.err.println(e.getMessage());
        }

    }
}
