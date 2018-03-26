package userCF.step1;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Yuqi Li
 * date: Mar 23, 2018 4:08:38 PM
 */
public class Reducer1 extends Reducer<Text, Text, Text, Text>{
	
	private Text outKey = new Text();
	private Text outValue = new Text();
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String userID = key.toString();
		
		//<userID, score>
		Map<String, Integer> map = new HashMap<String, Integer>();
		for(Text value : values){
			String itemID = value.toString().split("_")[0];
			String score = value.toString().split("_")[1];
			
			if(map.get(itemID)==null){
				map.put(itemID, Integer.valueOf(score));
			}else{
				Integer preScore = map.get(itemID);
				map.put(itemID, preScore + Integer.valueOf(score));
			}
		}
		
		StringBuilder sb = new StringBuilder();
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			String itemID = entry.getKey();
			String score = entry.getValue().toString();
			sb.append(itemID + "_" + score + ",");
		}
		String line = null;
		if(sb.toString().endsWith(",")){
			line = sb.substring(0,sb.length() - 1);
		}
		
		outKey.set(userID);
		outValue.set(line);
		
		context.write(outKey, outValue);
	}
	
}
