package com.sxt.hadoop.mr.tfidf;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class LastJob {

	public static void main(String[] args) {
		Configuration conf =new Configuration();
//		conf.set("mapred.jar", "C:\\Users\\root\\Desktop\\tfidf.jar");
		conf.set("mapreduce.job.jar", "C:\\Users\\root\\Desktop\\tfidf.jar");
		
		conf.set("mapreduce.app-submission.cross-platform", "true");
		
		
		try {
			FileSystem fs =FileSystem.get(conf);
			Job job =Job.getInstance(conf);
			job.setJarByClass(LastJob.class);
			job.setJobName("weibo3");
			job.setJar("C:\\Users\\root\\Desktop\\tfidf.jar");
			
			//2.5
			//把微博总数加载到
			job.addCacheFile(new Path("/data/tfidf/output/weibo1/part-r-00003").toUri());
			//把df加载到
			job.addCacheFile(new Path("/data/tfidf/output/weibo2/part-r-00000").toUri());
			
			
			
			
			
			//设置map任务的输出key类型、value类型
			job.setOutputKeyClass(Text.class);
			job.setOutputValueClass(Text.class);
			job.setMapperClass(LastMapper.class);
			job.setReducerClass(LastReduce.class);
			
			//mr运行时的输入数据从hdfs的哪个目录中获取
			FileInputFormat.addInputPath(job, new Path("/data/tfidf/output/weibo1"));
			Path outpath =new Path("/data/tfidf/output/weibo3");
			if(fs.exists(outpath)){
				fs.delete(outpath, true);
			}
			FileOutputFormat.setOutputPath(job,outpath );
			
			boolean f= job.waitForCompletion(true);
			if(f){
				System.out.println("执行job成功");
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
