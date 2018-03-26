package hadoopPractical;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Count the totals number of entries
 * @author Yuqi Li
 *
 */
public class Mapper2 extends Mapper<Object, Text, Text, IntWritable> {

	private Text word = new Text();
	Pattern p = Pattern.compile(" ");

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		word.set("Total numbers of entries: ");
		context.write(word, new IntWritable(1)); // map output
	}
}
