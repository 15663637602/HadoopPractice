package contentCF.step2;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

/**矩阵 乘（用处理过的矩阵作为缓存）
 * @author Yuqi Li date: Mar 23, 2018 5:17:29 PM
 */
public class MR2 {
	private static String inPath = "/ContentCF/step2_input";
	private static String outPath = "/ContentCF/step2_output";
	// hdfs address
	private static String hdfs = "hdfs://master:9000";
	// 将第一步输出的转置矩阵作为全局缓存
	private static String cache = "/ContentCF/step1_output/part-r-00000";

	public int run() {
		try {
			// job configuration
			Configuration conf = new Configuration();
			// set hdfs address
			conf.set("fs.defaultFS", hdfs);
			// create a job instance
			Job job = Job.getInstance(conf, "step1");
			//添加分布式缓存文件
			job.addCacheArchive(new URI(cache+"#itemUserScore2"));
			// set main class
			job.setJarByClass(MR2.class);
			job.setMapperClass(Mapper2.class);
			job.setReducerClass(Reducer2.class);
			// set Mapper output type
			job.setMapOutputKeyClass(Text.class);
			job.setMapOutputValueClass(Text.class);
			// set Reducer output type
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			FileSystem fs = FileSystem.get(conf);
			Path inputPath = new Path(inPath);
			if (fs.exists(inputPath)) {
				FileInputFormat.addInputPath(job, inputPath);
			}

			Path outputPath = new Path(outPath);
			// 如果文件存在才会删除
			fs.delete(outputPath, true);

			FileOutputFormat.setOutputPath(job, outputPath);
			// Success return 1, otherwise return -1
			return job.waitForCompletion(true) ? 1 : -1;

		} catch (IOException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (InterruptedException e) {
			e.printStackTrace();
		} catch (URISyntaxException e) {
			e.printStackTrace();
		}
		return -1;
	}
	
	public static void main(String[] args) {
		int result = -1;
		result = new MR2().run();
		if(result == 1){
			System.out.println("Success");
		}else{
			System.out.println("Failure");
		}
	}
}
