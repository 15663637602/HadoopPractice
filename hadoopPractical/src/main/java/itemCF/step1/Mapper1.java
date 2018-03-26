package itemCF.step1;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Yuqi Li
 * date: Mar 23, 2018 3:50:15 PM
 */
public class Mapper1 extends Mapper<LongWritable, Text, Text, Text>{
	private Text outKey = new Text();
	private Text outValue = new Text();
	
	/**
	 * key:1,2,3 (row number)
	 * value:A,1,1   ; C,3,5;   ....
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		String[] values = value.toString().split(",");
		String userID = values[0];
		String itemID = values[1];
		String score = values[2];
		
		outKey.set(itemID);
		outValue.set(userID+"_"+score);
		
	}
	
}
