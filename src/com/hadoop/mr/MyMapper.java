package com.hadoop.mr;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MyMapper extends Mapper<Object, Text, Text, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
//	private Text word = new Text();
	
	// key是行的偏移量
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		final String[] split = value.toString().split(" ");
	     
	     for (String word : split) {
	       context.write(new Text(word), one);
	     }
	     
//	     StringTokenizer itr = new StringTokenizer(value.toString());
//	     while (itr.hasMoreTokens()) {
//	       word.set(itr.nextToken());
//	       context.write(word, one);
//	     }
	 }
}