package com.sxt.hadoop.mr.tq;

import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.util.StringUtils;

public class TqMapper extends Mapper<LongWritable, Text, TQ, Text> {
	
	
	
	TQ tq= new TQ();
	Text vwd = new Text();
	
	@Override
	protected void map(LongWritable key, Text value,Context context)
			throws IOException, InterruptedException {
		
		
		//value:  1949-10-01 14:21:02	34c  >>  TQ

		try {
			String[] strs = StringUtils.split(value.toString(), '\t');
			
			SimpleDateFormat  sdf = new SimpleDateFormat("yyyy-MM-dd");
			Date date = null;
		
			date = sdf.parse(strs[0]);
			
			Calendar  cal = Calendar.getInstance();
			cal.setTime(date);
			
			tq.setYear(cal.get(Calendar.YEAR));
			tq.setMonth(cal.get(Calendar.MONTH)+1);
			tq.setDay(cal.get(Calendar.DAY_OF_MONTH));
			
			int wd = Integer.parseInt(strs[1].substring(0, strs[1].length()-1));
			tq.setWd(wd);
			vwd.set(wd+"");
			
			context.write(tq, vwd);
			
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
				
		
		
		
	}
	

}
