package com.sxt.hadoop.mr.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class TqSortComparator  extends  WritableComparator {

	
	
	public TqSortComparator() {
		super(TQ.class,true);
	}
	
	
	@Override
	public int compare(WritableComparable a, WritableComparable b) {
		
		TQ t1 = (TQ)a;
		TQ t2 = (TQ)b;

		int c1=Integer.compare(t1.getYear(), t2.getYear());
		if(c1==0){
			int c2=Integer.compare(t1.getMonth(), t2.getMonth());
			if(c2==0){
				return -Integer.compare(t1.getWd(), t2.getWd());
			}
			return c2;
		}
		
		return c1;
		
		
	}
	
	
	
	
}
