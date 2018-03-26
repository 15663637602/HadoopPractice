package hadoopPractical;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Count the number of entries for each IP
 * @author Yuqi Li
 *
 */
public class Mapper1 extends Mapper<Object, Text, Text, IntWritable> {

	private Text word = new Text();
	Pattern p = Pattern.compile(" ");

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String s[] = p.split(value.toString(), -1); // split data by " "
		word.set(s[0]);
		context.write(word, new IntWritable(1)); // map output
	}
}
