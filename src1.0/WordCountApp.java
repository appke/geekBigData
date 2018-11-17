package mapreduce;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountApp {
	
	static String INPUT_PATH = "hdfs://centos:9000/hello"; // 文件
	static String OUT_PATH = "hdfs://centos:9000/out";

	public  static void main(String[] args)  throws Exception{
	
		Configuration conf = new Configuration(); //加载配置文件
		final FileSystem fs = FileSystem.get(new URI(INPUT_PATH), conf);
		
		Path outPath = new Path(OUT_PATH);
		if (fs.exists(outPath)){
			fs.delete(outPath, true);
		}
		
		//创建一个job，供JobTracker使用
		final Job job = new Job(conf, WordCountApp.class.getSimpleName());
		
		//开始天龙八部
		// 1.1指定读取的文件位于哪里
		FileInputFormat.setInputPaths(job, INPUT_PATH);
		
		// 1.2 指定自定义的map类
		job.setMapperClass(MyMapper.class);
		//map输出的<k,v>类型。如果<k3,v3>的类型与<k2,v2>类型一致，则可以省略
		//job.setMapOutputKeyClass(Text.class);
		//job.setMapOutputValueClass(LongWritable.class);
		
		//1.3 分区
		//job.setPartitionerClass(HashPartitioner.class);
		//有一个reduce任务运行
		//job.setNumReduceTasks(1);
		
		//1.4 TODO 排序、分组
		//1.5 TODO 规约
		
		// 2.2指定自定义的reduce类
		job.setReducerClass(MyReduce.class);
		// 指定reduce的输出类型
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(LongWritable.class);
		
		// 2.3指定输出到哪里
		FileOutputFormat.setOutputPath(job, outPath);
		
		// 把job提交给JobTracker运行
		job.waitForCompletion(true);

		
	}
	
	/**
	 * KEYIN	即k1		表示行的偏移量
	 * VALUEIN	即v1		表示行文本内容
	 * KEYOUT	即k2		表示行中出现的单词
	 * VALUEOUT	即v2		表示行中出现的单词的次数，固定值1
	 */
	static class MyMapper extends Mapper<LongWritable, Text, Text, LongWritable> {

		protected void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			final String [] split = value.toString().split(" ");
			for (String word : split) {
				context.write(new Text(word), new LongWritable(1L));
			}
		}
	}
	
	/**
	 * KEYIN	即k2		表示行中出现的单词
	 * VALUEIN	即v2		表示行中出现的单词的次数
	 * KEYOUT	即k3		表示文本中出现的不同单词
	 * VALUEOUT	即v3		表示文本中出现的不同单词的总次数
	 *
	 */
	static class MyReduce extends Reducer<Text, LongWritable, Text, LongWritable>{

		protected void reduce(Text k2, Iterable<LongWritable> v2s,Context context) throws IOException, InterruptedException {

			long count = 0L;
			for (LongWritable v2 : v2s) {
				count += v2.get();
			}
			context.write(k2, new LongWritable(count));
		}
	}

}
