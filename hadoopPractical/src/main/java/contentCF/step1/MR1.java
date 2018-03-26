package contentCF.step1;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import com.sun.jersey.core.impl.provider.entity.XMLJAXBElementProvider.Text;

/**
 * 矩阵转置
 * @author Yuqi Li date: Mar 23, 2018 4:16:09 PM
 */
public class MR1 {
	private static String inPath = "/ContentCF/step1_output/";
	private static String outPath = "/ContentCF/step1_output";
	// hdfs address
	private static String hdfs = "hdfs://master:9000";

	public int run() {
		try {
			// job configuration
			Configuration conf = new Configuration();
			// set hdfs address
			conf.set("fs.defaultFS", hdfs);
			// create a job instance
			Job job = Job.getInstance(conf, "step1");
			// set main class
			job.setJarByClass(MR1.class);
			job.setMapperClass(Mapper1.class);
			job.setReducerClass(Reducer1.class);
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
		}
		return -1;
	}
	
	public static void main(String[] args) {
		int result = -1;
		result = new MR1().run();
		if(result == 1){
			System.out.println("Success");
		}else{
			System.out.println("Failure");
		}
	}
}
