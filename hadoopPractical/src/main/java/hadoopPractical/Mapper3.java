package hadoopPractical;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Count the number of distinct IPs (Using the output of first result)
 * @author Yuqi Li
 *
 */
public class Mapper3 extends Mapper<Object, Text, Text, IntWritable> {

	private Text word = new Text();
	Pattern p = Pattern.compile(" ");

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		word.set("The number of distinct IPs: ");
		context.write(word, new IntWritable(1)); // map output
	}
}
