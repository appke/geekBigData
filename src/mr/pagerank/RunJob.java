package com.sxt.hadoop.mr.pagerank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.KeyValueTextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class RunJob {

	public static enum Mycounter {
		my
	}

	public static void main(String[] args) {
		
		Configuration conf = new Configuration(true);
		conf.set("mapreduce.app-submission.corss-paltform", "true");
		//如果分布式运行,必须打jar包
		//且,client在集群外非hadoop jar 这种方式启动,client中必须配置jar的位置
		conf.set("mapreduce.framework.name", "local");
		//这个配置,只属于,切换分布式到本地单进程模拟运行的配置
		//这种方式不是分布式,所以不用打jar包
		
		
		double d = 0.0000001;
		int i = 0;
		while (true) {
			i++;
			try {
				conf.setInt("runCount", i);
				
				FileSystem fs = FileSystem.get(conf);
				Job job = Job.getInstance(conf);				
				job.setJarByClass(RunJob.class);
				job.setJobName("pr" + i);
				job.setMapperClass(PageRankMapper.class);
				job.setReducerClass(PageRankReducer.class);
				job.setMapOutputKeyClass(Text.class);
				job.setMapOutputValueClass(Text.class);
				
				//使用了新的输入格式化类
				job.setInputFormatClass(KeyValueTextInputFormat.class);
				
				
				Path inputPath = new Path("/data/pagerank/input/");
				
				if (i > 1) {
					inputPath = new Path("/data/pagerank/output/pr" + (i - 1));
				}
				FileInputFormat.addInputPath(job, inputPath);

				Path outpath = new Path("/data/pagerank/output/pr" + i);
				if (fs.exists(outpath)) {
					fs.delete(outpath, true);
				}
				FileOutputFormat.setOutputPath(job, outpath);

				boolean f = job.waitForCompletion(true);
				if (f) {
					System.out.println("success.");
					long sum = job.getCounters().findCounter(Mycounter.my).getValue();
					
					System.out.println(sum);
					double avgd = sum / 4000.0;
					if (avgd < d) {
						break;
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	static class PageRankMapper extends Mapper<Text, Text, Text, Text> {
		protected void map(Text key, Text value, Context context) throws IOException, InterruptedException {
			
			int runCount = context.getConfiguration().getInt("runCount", 1);
			
			//A	   B D
			//K:A
			//V:B D
			//K:A
			//V:0.3 B D
			String page = key.toString();
			Node node = null;
			if (runCount == 1) {
				node = Node.fromMR("1.0" , value.toString());
			} else {
				node = Node.fromMR(value.toString());
			}
			// A:1.0 B D  传递老的pr值和对应的页面关系
			context.write(new Text(page), new Text(node.toString()));
			
			if (node.containsAdjacentNodes()) {
				double outValue = node.getPageRank() / node.getAdjacentNodeNames().length;
				for (int i = 0; i < node.getAdjacentNodeNames().length; i++) {
					String outPage = node.getAdjacentNodeNames()[i];
					// B:0.5
					// D:0.5    页面A投给谁，谁作为key，val是票面值，票面值为：A的pr值除以超链接数量
					context.write(new Text(outPage), new Text(outValue + ""));
				}
			}
		}
	}

	static class PageRankReducer extends Reducer<Text, Text, Text, Text> {
		protected void reduce(Text key, Iterable<Text> iterable, Context context)
				throws IOException, InterruptedException {
			
			//相同的key为一组
			//key：页面名称比如B 
			//包含两类数据
			//B:1.0 C  //页面对应关系及老的pr值
			
			//B:0.5		//投票值
			//B:0.5
			
			
			double sum = 0.0;
			
			Node sourceNode = null;
			for (Text i : iterable) {
				Node node = Node.fromMR(i.toString());
				if (node.containsAdjacentNodes()) {
					sourceNode = node;
				} else {
					sum = sum + node.getPageRank();
				}
			}

			// 4为页面总数
			double newPR = (0.15 / 4.0) + (0.85 * sum);
			System.out.println("*********** new pageRank value is " + newPR);

			// 把新的pr值和计算之前的pr比较
			double d = newPR - sourceNode.getPageRank();

			int j = (int) (d * 1000.0);
			j = Math.abs(j);
			System.out.println(j + "___________");
			context.getCounter(Mycounter.my).increment(j);

			sourceNode.setPageRank(newPR);
			context.write(key, new Text(sourceNode.toString()));
		}
	}
}
