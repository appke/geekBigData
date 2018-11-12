package com.sxt.hadoop.mr.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FReducer  extends  Reducer<Text, IntWritable, Text, Text> {
	
	Text rval = new Text();
	@Override
	protected void reduce(Text key, Iterable<IntWritable> vals, Context context)
			throws IOException, InterruptedException {
		//hadoop:hello  1
		//hadoop:hello  0
		//hadoop:hello  1
		//hadoop:hello  1
		int sum=0;
		int flg=0;
		for (IntWritable v : vals) {
			if(v.get()==0){
				//hadoop:hello  0
				flg=1;
			}
			sum+=v.get();
		}
		if(flg==0){
			rval.set(sum+"");
			context.write(key, rval);
		}
		
		
		
		
		
	}

}
