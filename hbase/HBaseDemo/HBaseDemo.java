package com.bjsxt.hbase;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.bjsxt.hbase.Phone.PhoneDetail;

public class HBaseDemo {

	HBaseAdmin admin;
	HTable htable;
	String TN = "phone";

	@Before
	public void init() throws Exception {
		Configuration conf = new Configuration();
		conf.set("hbase.zookeeper.quorum", "node1,node2,node3");
		admin = new HBaseAdmin(conf);
		htable = new HTable(conf, TN.getBytes());
	}

	@Test
	public void creatTable() throws Exception {

		if (admin.tableExists(TN)) {
			admin.disableTable(TN);
			admin.deleteTable(TN);
		}

		// 表描述
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TN));
		HColumnDescriptor cf = new HColumnDescriptor("cf".getBytes());
		desc.addFamily(cf);
		admin.createTable(desc);
	}

	@Test
	public void insertDB() throws Exception {
		String rowKey = "1231231312";
		Put put = new Put(rowKey.getBytes());
		put.add("cf".getBytes(), "name".getBytes(), "xiaohong".getBytes());
		put.add("cf".getBytes(), "age".getBytes(), "23".getBytes());
		put.add("cf".getBytes(), "sex".getBytes(), "women".getBytes());
		htable.put(put);
	}

	SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");

	/**
	 * 有10个用户，每个用户随机产生100条记录
	 * 
	 * @throws Exception
	 */
	@Test
	public void insertDB2() throws Exception {
		List<Put> puts = new ArrayList<Put>();
		for (int i = 0; i < 10; i++) {
			String phoneNum = getPhoneNum("186");
			for (int j = 0; j < 100; j++) {
				String dnum = getPhoneNum("158");
				String length = r.nextInt(99) + "";
				String type = r.nextInt(2) + "";
				String dateStr = getDate("2018");
				String rowkey = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse(dateStr).getTime());
				Put put = new Put(rowkey.getBytes());
				put.add("cf".getBytes(), "dnum".getBytes(), dnum.getBytes());
				put.add("cf".getBytes(), "length".getBytes(), length.getBytes());
				put.add("cf".getBytes(), "type".getBytes(), type.getBytes());
				put.add("cf".getBytes(), "date".getBytes(), dateStr.getBytes());
				puts.add(put);
			}
		}
		htable.put(puts);
	}

	@Test
	public void insertDB3() throws Exception {
		List<Put> puts = new ArrayList<Put>();
		for (int i = 0; i < 10; i++) {
			String phoneNum = getPhoneNum("186");
			for (int j = 0; j < 100; j++) {
				String dnum = getPhoneNum("158");
				String length = r.nextInt(99) + "";
				String type = r.nextInt(2) + "";
				String dateStr = getDate("2018");
				String rowkey = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse(dateStr).getTime());
				Phone2.PhoneDetail.Builder phoneDetail = Phone2.PhoneDetail.newBuilder();
				phoneDetail.setDate(dateStr);
				phoneDetail.setDnum(dnum);
				phoneDetail.setLength(length);
				phoneDetail.setType(type);
				Put put = new Put(rowkey.getBytes());
				put.add("cf".getBytes(), "phoneDetail".getBytes(), phoneDetail.build().toByteArray());
				puts.add(put);
			}
		}
		htable.put(puts);
	}

	/**
	 * 有十个用户，每个用户每天产生100条记录，将100条记录放到一个集合进行存储
	 * 
	 * @throws Exception
	 */
	@Test
	public void insertDB4() throws Exception {
		List<Put> puts = new ArrayList<Put>();
		for (int i = 0; i < 10000; i++) {
			String phoneNum = getPhoneNum("186");
			String rowkey = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse(getDate2("20180529")).getTime());
			Phone.dayPhoneDetail.Builder dayPhone = Phone.dayPhoneDetail.newBuilder();
			for (int j = 0; j < 100; j++) {
				String dnum = getPhoneNum("158");
				String length = r.nextInt(99) + "";
				String type = r.nextInt(2) + "";
				String dateStr = getDate("2018");
				Phone.PhoneDetail.Builder phoneDetail = Phone.PhoneDetail.newBuilder();
				phoneDetail.setDate(dateStr);
				phoneDetail.setDnum(dnum);
				phoneDetail.setLength(length);
				phoneDetail.setType(type);
				dayPhone.addDayPhoneDetail(phoneDetail);
			}
			Put put = new Put(rowkey.getBytes());
			put.add("cf".getBytes(), "day".getBytes(), dayPhone.build().toByteArray());
			puts.add(put);
		}
		htable.put(puts);
	}

	@Test
	public void getDB2() throws Exception{
		Get get = new Get("18686966381_9223370509257224807".getBytes());
		Result result = htable.get(get);
		Cell cell = result.getColumnLatestCell("cf".getBytes(), "day".getBytes());
		Phone.dayPhoneDetail dayPhone = Phone.dayPhoneDetail.parseFrom(CellUtil.cloneValue(cell));
		for (PhoneDetail pd : dayPhone.getDayPhoneDetailList()) {
			System.out.println(pd);
		}
	}
	
	
	private String getDate(String year) {
		return year + String.format("%02d%02d%02d%02d%02d",
				new Object[] { r.nextInt(12) + 1, r.nextInt(31) + 1, r.nextInt(24), r.nextInt(60), r.nextInt(60) });
	}

	private String getDate2(String yearMonthDay) {
		return yearMonthDay
				+ String.format("%02d%02d%02d", new Object[] { r.nextInt(24), r.nextInt(60), r.nextInt(60) });
	}

	Random r = new Random();

	/**
	 * 生成随机的手机号码
	 * 
	 * @param string
	 * @return
	 */
	private String getPhoneNum(String string) {
		return string + String.format("%08d", r.nextInt(99999999));
	}

	@Test
	public void getDB() throws Exception {
		String rowKey = "1231231312";
		Get get = new Get(rowKey.getBytes());
		get.addColumn("cf".getBytes(), "name".getBytes());
		get.addColumn("cf".getBytes(), "age".getBytes());
		get.addColumn("cf".getBytes(), "sex".getBytes());
		Result rs = htable.get(get);
		Cell cell = rs.getColumnLatestCell("cf".getBytes(), "name".getBytes());
		Cell cell2 = rs.getColumnLatestCell("cf".getBytes(), "age".getBytes());
		Cell cell3 = rs.getColumnLatestCell("cf".getBytes(), "sex".getBytes());
		// System.out.println(new String(cell.getValue()));
		System.out.println(new String(CellUtil.cloneValue(cell)));
		System.out.println(new String(CellUtil.cloneValue(cell2)));
		System.out.println(new String(CellUtil.cloneValue(cell3)));

	}

	/**
	 * 统计二月份到三月份的通话记录
	 * 
	 * @throws Exception
	 */
	@Test
	public void scan() throws Exception {
		String phoneNum = "18676604687";
		String startRow = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse("20180301000000").getTime());
		String stopRow = phoneNum + "_" + (Long.MAX_VALUE - sdf.parse("20180201000000").getTime());
		Scan scan = new Scan();
		scan.setStartRow(startRow.getBytes());
		scan.setStopRow(stopRow.getBytes());
		ResultScanner rss = htable.getScanner(scan);
		for (Result rs : rss) {
			System.out
					.print(new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
			System.out.print("-"
					+ new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
			System.out.print(
					"-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
			System.out.println(
					"-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
		}
	}

	/**
	 * 查询某个手机号主叫为1 的所有记录
	 * 
	 * @throws Exception
	 */
	@Test
	public void scan2() throws Exception {
		FilterList list = new FilterList(FilterList.Operator.MUST_PASS_ALL);
		PrefixFilter filter1 = new PrefixFilter("18676604687".getBytes());
		SingleColumnValueFilter filter2 = new SingleColumnValueFilter("cf".getBytes(), "type".getBytes(),
				CompareOp.EQUAL, "1".getBytes());
		list.addFilter(filter1);
		list.addFilter(filter2);
		Scan scan = new Scan();
		scan.setFilter(list);
		ResultScanner rss = htable.getScanner(scan);
		for (Result rs : rss) {
			System.out
					.print(new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "dnum".getBytes()))));
			System.out.print("-"
					+ new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "length".getBytes()))));
			System.out.print(
					"-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "type".getBytes()))));
			System.out.println(
					"-" + new String(CellUtil.cloneValue(rs.getColumnLatestCell("cf".getBytes(), "date".getBytes()))));
		}
	}

	@After
	public void destory() throws Exception {
		if (admin != null) {
			admin.close();
		}
	}
}
