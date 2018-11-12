package com.sxt.hadoop.mr.fof;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class FMapper extends Mapper<LongWritable, Text, Text, IntWritable>{

	Text mkey= new Text();
	IntWritable mval = new IntWritable();
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		//value:
		//tom       hello hadoop cat   :   hello:hello  1
		//hello     tom world hive mr      hello:hello  0


		String[] strs = StringUtils.split(value.toString(), ' ');
		
		String user=strs[0];
		String user01=null;
		for(int i=1;i<strs.length;i++){
			
			
			mkey.set(fof(strs[0],strs[i]));  
			mval.set(0); 
			
			context.write(mkey, mval);  
			
			for (int j = i+1; j < strs.length; j++) {
				Thread.sleep(context.getConfiguration().getInt("sleep", 0));
				mkey.set(fof(strs[i],strs[j]));  
				mval.set(1);  
				context.write(mkey, mval);  
				
			}
		}
		
		
		
		
	}
	
	public static String fof(String str1  , String str2){
		
		
		if(str1.compareTo(str2) > 0){
			//hello,hadoop
			return str2+":"+str1;
			//hadoop:hello
		}
			
		
		//hadoop,hello
		return str1+":"+str2;
		//hadoop:hello
		
		
		
	}
	
	
	
	
	
	
}
