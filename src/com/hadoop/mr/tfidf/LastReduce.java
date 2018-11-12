package com.sxt.hadoop.mr.tfidf;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class LastReduce extends Reducer<Text, Text, Text, Text> {

	protected void reduce(Text key, Iterable<Text> iterable, Context context)
			throws IOException, InterruptedException {

		StringBuffer sb = new StringBuffer();

		for (Text i : iterable) {
			sb.append(i.toString() + "\t");
		}

		context.write(key, new Text(sb.toString()));
	}

}
