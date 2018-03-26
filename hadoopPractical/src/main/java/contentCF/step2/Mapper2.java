package contentCF.step2;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

/**
 * @author Yuqi Li
 * date: Mar 23, 2018 4:35:16 PM
 */
public class Mapper2 extends Mapper<LongWritable, Text, Text, Text>{

	private Text outKey = new Text();
	private Text outValue = new Text();
	private List<String> cacheList = new ArrayList<String>();
	
	private DecimalFormat df = new DecimalFormat("0.00");
	
	//setup在map循环执行之前执行，且只执行一次，
	//在这里加载右边的整个矩阵到list中，在map方法里就可以使用list
	@Override
	protected void setup(Context context)
			throws IOException, InterruptedException {
		super.setup(context);
		//通过输入流将之后要放入全局缓存中的 右侧矩阵 读入List<String> 中
		FileReader fr = new FileReader("itemUserScore2");
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
		//Row
		String row_matrix1 = value.toString().split("\t")[0];
		//Column_Value
		String[] column_value_matrix1 = value.toString().split("\t")[1].split(",");
		
		//让map获取到的这一行（左边矩阵的某一行）和右边整个矩阵相乘
		for(String line : cacheList){
			//line 是右侧矩阵的每一行
			String row_matrix2 = line.toString().split("\t")[0];
			String[] column_value_matrix2 = line.toString().split("\t")[1].split(",");
			//矩阵两个行向量相乘的结果
			double result = 0;
			//遍历左矩阵的每一列
			for(String column_value_1:column_value_matrix1){
				String column_matrix1 = column_value_1.split("_")[0];
				String value_matrix1 = column_value_1.split("_")[1];
				//遍历右矩阵每一行的每一列
				for(String column_value_2:column_value_matrix2){
					//列要相等才能进行乘积运算
					if(column_value_2.startsWith(column_matrix1+"_")){
						String value_matrix2 = column_value_2.split("_")[1];
						//相乘并累加
						result += Double.valueOf(value_matrix1) * Double.valueOf(value_matrix2);
					}
				}
			}
			
			if( result == 0){
				continue;
			}
			//result 的坐标是 行：matrix1 对应的行,列：matrix2对应的行
			outKey.set(row_matrix1);
			outValue.set(row_matrix2+"_"+df.format(result));
			//输出格式 Key：行，value：列_值
			context.write(outKey, outValue);
		}
		
	}
}
