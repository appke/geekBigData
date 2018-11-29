package com.sxt.hadoop.mr.tq;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

public class TQ implements  WritableComparable<TQ>{

	private int year;
	private int month;
	private int day;
	private int wd;
	
	
	public int getYear() {
		return year;
	}

	public void setYear(int year) {
		this.year = year;
	}

	public int getMonth() {
		return month;
	}

	public void setMonth(int month) {
		this.month = month;
	}

	public int getDay() {
		return day;
	}

	public void setDay(int day) {
		this.day = day;
	}

	public int getWd() {
		return wd;
	}

	public void setWd(int wd) {
		this.wd = wd;
	}

	@Override
	public void write(DataOutput out) throws IOException {
		out.writeInt(year);
		out.writeInt(month);
		out.writeInt(day);
		out.writeInt(wd);
		
	}

	@Override
	public void readFields(DataInput in) throws IOException {
		this.year=in.readInt();
		this.month=in.readInt();
		this.day=in.readInt();
		this.wd=in.readInt();
	}

	@Override
	public int compareTo(TQ that) {
		
		int c1=Integer.compare(this.getYear(), that.getYear());
		if(c1==0){
			int c2=Integer.compare(this.getMonth(), that.getMonth());
			if(c2==0){
				return Integer.compare(this.getDay(), that.getDay());
			}
			return c2;
		}
		
		return c1;
	}
	

}
