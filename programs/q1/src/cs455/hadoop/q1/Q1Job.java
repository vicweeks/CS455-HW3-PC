package cs455.hadoop.q1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;

import java.io.IOException;

/*
 * This is the main class. Hadoop will invoke the main method of this class.
 */
public class Q1Job {
 
    public static void main(String[] args) {
        try {
            Configuration conf = new Configuration();
            // Give the MapRed job a name. You'll see this name in the Yarn webapp.
            Job job = Job.getInstance(conf, "Q1");
            // Current class.
            job.setJarByClass(Q1Job.class);
            // Mapper
            job.setMapperClass(Q1Mapper.class);
	    // Combiner. None used for this case.	    
            //job.setCombinerClass(Q1Reducer.class);
	    // Reducer
            job.setReducerClass(Q1Reducer.class);
            // Outputs from the Mapper.
            job.setMapOutputKeyClass(Text.class);
            job.setMapOutputValueClass(IntWritable.class);
            // Outputs from Reducer. It is sufficient to set only the following two properties
            // if the Mapper and Reducer has same key and value types. It is set separately for
            // elaboration.
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            // path to input in HDFS
            FileInputFormat.addInputPath(job, new Path("/data/main"));
            // path to output in HDFS
            FileOutputFormat.setOutputPath(job, new Path("/home/output-1"));
	    //MultipleOutputs.addNamedOutput(job, "file", FileOutputFormat.class,
	    //				   Text.class, IntWritable.class);
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
