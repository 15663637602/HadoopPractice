package userCF.step5;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.DF;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Yuqi Li
 * date: Mar 23, 2018 4:35:16 PM
 */
public class Mapper5 extends Mapper<LongWritable, Text, Text, Text>{

	private Text outKey = new Text();
	private Text outValue = new Text();
	private List<String> cacheList = new ArrayList<String>();
	
	//setup在map循环执行之前执行，且只执行一次，
	//在这里加载右边的整个矩阵到list中，在map方法里就可以使用list
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		//通过输入流将之后要放入全局缓存中的  评分矩阵（step1的结果） 读入List<String> 中
		FileReader fr = new FileReader("itemUserScore3");
		BufferedReader br = new BufferedReader(fr);
		
		//每行的格式: 行 tab 列_值,列_值,列_值,列_值
		String line = null;
		while((line = br.readLine())!= null){
			cacheList.add(line);
		}
		fr.close();
		br.close();
	}

	/**
	 * Key: 行号, Value: 行 tab 列_值,列_值,列_值,列_值
	 */
	@Override
	protected void map(LongWritable key, Text value, Context context)
			throws IOException, InterruptedException {
		//推荐列表矩阵（矩阵1，step4的结果）的行
		String itemID_1 = value.toString().split("\t")[0];
		String[] user_score_array1 = value.toString().split("\t")[1].split(",");
		//遍历评分列表（矩阵2）的行
		for(String line : cacheList){
			String itemID_2 = line.split("\t")[0];
			String[] user_score_array2 = line.split("\t")[1].split(",");
			
			//行首的id相同即itemID相同，才进行下一步
			if(itemID_1.equals(itemID_2)){
				//遍历矩阵1的  列
				for(String user_score_1 : user_score_array1){
					boolean flag = false;
					String user_1 = user_score_1.split("_")[0];
					String score_1 = user_score_1.split("_")[1];
					
					//遍历矩阵2的  列
					for(String user_score_2 : user_score_array2){
						String user_2 = user_score_2.split("_")[0];
						if(user_1.equals(user_2)){//如果有用户名相同，说明矩阵2中存在用户对该物品的记录
							flag = true;
						}
					}
					
					if(flag = false){//如果不存在用户对该物品的记录，那么就可以推荐
						outKey.set(user_1);
						outValue.set(itemID_1+"_"+score_1);
						context.write(outKey, outValue);
					}
				}
			}
			
		}
	}
}
