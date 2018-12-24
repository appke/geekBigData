package com.bjsxt.wc;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class WCRunner {

	public static void main(String[] args) throws Exception {

		Configuration conf = new Configuration();
		conf.set("fs.defaultFS", "hdfs://node1:8020");
		conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
		Job job = Job.getInstance(conf);
		job.setJarByClass(WCRunner.class);

		// 指定mapper 和 reducer
		job.setMapperClass(WCMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		// 最后一个参数设置false
		// TableMapReduceUtil.initTableReducerJob(table, reducer, job);
		TableMapReduceUtil.initTableReducerJob("wc", WCReducer.class, job, null, null, null, null, false);
		FileInputFormat.addInputPath(job, new Path("/user/hive/warehouse/wc/"));
		job.waitForCompletion(true);
	}
}
