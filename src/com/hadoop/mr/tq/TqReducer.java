package com.sxt.hadoop.mr.tq;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class TqReducer extends Reducer<TQ, Text, Text, Text> {
	
	Text rkey = new Text();
	Text rval = new Text();
	
	
	@Override
	protected void reduce(TQ key, Iterable<Text> vals, Context context)
			throws IOException, InterruptedException {
		
		
		int flg=0;
		int day=0;
		
		for (Text v : vals) {
			
			if(flg==0){
				day=key.getDay();	
				
				rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
				rval.set(key.getWd()+"");
				context.write(rkey,rval );
				flg++;
				
			}
			if(flg!=0 && day != key.getDay()){
				
				rkey.set(key.getYear()+"-"+key.getMonth()+"-"+key.getDay());
				rval.set(key.getWd()+"");
				context.write(rkey,rval );
				break;
			}
				
			
			
			
		}
		
		
	}

}
