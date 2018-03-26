package hadoopPractical;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reducer1 extends Reducer<Text, IntWritable, Text, IntWritable> {
	private IntWritable v = new IntWritable();
	private int sum = 0;

	public void reduce(Text key, Iterable<IntWritable> values, Context context)
			throws IOException, InterruptedException {
		sum = 0;
		for (IntWritable val : values) {
			sum += val.get(); // accumulation
		}
		v.set(sum);
		context.write(key, v); // reduce output
	}
}
