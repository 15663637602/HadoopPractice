package distinct_words;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * Count the number of distinct words (Using the output of first result)
 * @author Yuqi Li
 *
 */
public class Mapper2 extends Mapper<Object, Text, Text, IntWritable> {

	private Text word = new Text();

	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		word.set("The number of distinct words: ");
		context.write(word, new IntWritable(1)); // map output
	}
}
