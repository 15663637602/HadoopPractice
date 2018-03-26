package hadoopPractical;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Driver {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		//job1: Count the number of entries for each IP
		Job job = new Job(conf);
		job.setJarByClass(Driver.class);
		job.setJobName("Count the number of entries for each IP");
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		job.setMapperClass(Mapper1.class);
		job.setReducerClass(Reducer1.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
		//job2: Count the totals number of entries
		Job job2 = new Job(conf);
		job2.setJarByClass(Driver.class);
		job2.setJobName("Count the totals number of entries");
		job2.setOutputKeyClass(Text.class);
		job2.setOutputValueClass(IntWritable.class);
		job2.setMapperClass(Mapper2.class);
		job2.setReducerClass(Reducer1.class);
		job2.setInputFormatClass(TextInputFormat.class);
		job2.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job2, new Path(args[0]));
		FileOutputFormat.setOutputPath(job2, new Path(args[2]));
		job2.waitForCompletion(true);
		//job3: Count the number of distinct IPs (Using the output of first result)
		Job job3 = new Job(conf);
		job3.setJarByClass(Driver.class);
		job3.setJobName("Count the number of distinct IPs");
		job3.setOutputKeyClass(Text.class);
		job3.setOutputValueClass(IntWritable.class);
		job3.setMapperClass(Mapper3.class);
		job3.setReducerClass(Reducer1.class);
		job3.setInputFormatClass(TextInputFormat.class);
		job3.setOutputFormatClass(TextOutputFormat.class);
		FileInputFormat.addInputPath(job3, new Path(args[1]));
		FileOutputFormat.setOutputPath(job3, new Path(args[3]));
		job3.waitForCompletion(true);
	}
}
