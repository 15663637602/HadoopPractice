package userCF.step3;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Yuqi Li
 * date: Mar 23, 2018 3:50:15 PM
 */
public class Mapper3 extends Mapper<LongWritable, Text, Text, Text>{
	private Text outKey = new Text();
	private Text outValue = new Text();
	
	/**
	 * key:1
	 * value:1	1_0,2_3,3_-1,4_2,5_-3
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		// TODO Auto-generated method stub
		String[] rowAndLine = value.toString().split("\t");
		// row number
		String row = rowAndLine[0];
		String[] lines = rowAndLine[1].split(",");
		for(int i = 0;i<lines.length;i++){
			String column = lines[i].split("_")[0];
			String valueStr = lines[i].split("_")[1];
			//key: 变成列号， value：行号_值
			outKey.set(column);
			outValue.set(row+"_"+valueStr);
			context.write(outKey, outValue);
		}
		
	}
	
}
