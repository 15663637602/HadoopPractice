package distinct_words;

import java.io.IOException;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Yuqi Li
 * date: Mar 2, 2018 2:28:47 PM
 */
public class Mapper_zZ extends Mapper<Object, Text, Text, IntWritable>{
	private Text word = new Text("The number of words start with the letter Z/z: ");
	Pattern p = Pattern.compile("[()/\\s+#,.;:?!\\*\\[\\]]");
	
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

		String s[] = p.split(value.toString(), -1); // split data
		for(String ss: s){
			if(StringUtils.isNotBlank(ss)&&ss.startsWith("z")||ss.startsWith("Z")){
				context.write(word, new IntWritable(1)); // map output
			}
		}
		
	}
}
