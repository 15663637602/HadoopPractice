package itemCF.step1;

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
	//key: 列号， value：[行号_值,行号_值,行号_值,行号_值]
	@Override
	protected void reduce(Text key, Iterable<Text> values, Context context)
			throws IOException, InterruptedException {
		String itemID = key.toString();
		
		//<userID, score>
		Map<String, Integer> map = new HashMap<String, Integer>();
		for(Text value : values){
			String userID = value.toString().split("_")[0];
			String score = value.toString().split("_")[1];
			
			if(map.get(userID)==null){
				map.put(userID, Integer.valueOf(score));
			}else{
				Integer preScore = map.get(userID);
				map.put(userID, preScore + Integer.valueOf(score));
			}
		}
		
		StringBuilder sb = new StringBuilder();
		for(Map.Entry<String, Integer> entry : map.entrySet()){
			String userID = entry.getKey();
			String score = entry.getValue().toString();
			sb.append(userID + "_" + score + ",");
		}
		String line = null;
		if(sb.toString().endsWith(",")){
			line = sb.substring(0,sb.length() - 1);
		}
		
		outKey.set(itemID);
		outValue.set(line);
		
		context.write(outKey, outValue);
	}
	
}
