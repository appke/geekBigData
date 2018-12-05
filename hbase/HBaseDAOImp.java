package com.sxt.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HConnection;
import org.apache.hadoop.hbase.client.HConnectionManager;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.HTableInterface;
import org.apache.hadoop.hbase.client.HTablePool;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.BinaryComparator;
import org.apache.hadoop.hbase.filter.CompareFilter.CompareOp;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.filter.RowFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;
import org.apache.hadoop.hbase.filter.SubstringComparator;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;


public class HBaseDAOImp {

	HConnection hTablePool = null;
	static Configuration conf =null;
	public HBaseDAOImp()
	{
		 conf = new Configuration();
		String zk_list = "node1,node2,node3";
		conf.set("hbase.zookeeper.quorum", zk_list);
		try {
			hTablePool = HConnectionManager.createConnection(conf) ;
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public void save(Put put, String tableName) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			table.put(put) ;
			
		} catch (Exception e) {
			e.printStackTrace() ;
		}finally{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 插入一个cell
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param quailifer
	 * @param value
	 */
	public void insert(String tableName, String rowKey, String family,
			String quailifer, String value) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			Put put = new Put(rowKey.getBytes());
			put.add(family.getBytes(), quailifer.getBytes(), value.getBytes()) ;
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	/**
	 * 在一个列族下插入多个单元格
	 * @param tableName
	 * @param rowKey
	 * @param family
	 * @param quailifer
	 * @param value
	 */
	public void insert(String tableName,String rowKey,String family,String quailifer[],String value[])
	{
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			Put put = new Put(rowKey.getBytes());
			// 批量添加
			for (int i = 0; i < quailifer.length; i++) {
				String col = quailifer[i];
				String val = value[i];
				put.add(family.getBytes(), col.getBytes(), val.getBytes());
			}
			table.put(put);
		} catch (Exception e) {
			e.printStackTrace();
		}finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	public void save(List<Put> Put, String tableName) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			table.put(Put) ;
		}
		catch (Exception e) {
			// TODO: handle exception
		}finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}


	public Result getOneRow(String tableName, String rowKey) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		Result rsResult = null;
		try {
			table = hTablePool.getTable(tableName) ;
			Get get = new Get(rowKey.getBytes()) ;
			rsResult = table.get(get) ;
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return rsResult;
	}
	
	/**
	 * 最常用的方法，优化查询
	 * 查询一行数据，
	 * @param tableName
	 * @param rowKey
	 * @param cols
	 * @return
	 */
	public Result getOneRowAndMultiColumn(String tableName, String rowKey,String[] cols) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		Result rsResult = null;
		try {
			table = hTablePool.getTable(tableName) ;
			Get get = new Get(rowKey.getBytes()) ;
			for (int i = 0; i < cols.length; i++) {
				get.addColumn("cf".getBytes(), cols[i].getBytes()) ;
			}
			rsResult = table.get(get) ;
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return rsResult;
	}

	
	public List<Result> getRows(String tableName, String rowKeyLike) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		List<Result> list = null;
		try {
			FilterList fl = new FilterList(FilterList.Operator.MUST_PASS_ALL);
			table = hTablePool.getTable(tableName) ;
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			SingleColumnValueFilter filter1 = new SingleColumnValueFilter(
					  "order".getBytes(),
					  "order_type".getBytes(),
					  CompareOp.EQUAL,
					  Bytes.toBytes("1")
					  );
			fl.addFilter(filter);
			fl.addFilter(filter1);
			Scan scan = new Scan();
			scan.setFilter(fl);
			ResultScanner scanner = table.getScanner(scan) ;
			list = new ArrayList<Result>() ;
			for (Result rs : scanner) {
				list.add(rs) ;
			}
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	
	
	public List<Result> getRows(String tableName, String rowKeyLike ,String cols[]) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName) ;
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			
			Scan scan = new Scan();
			for (int i = 0; i < cols.length; i++) {
				scan.addColumn("cf".getBytes(), cols[i].getBytes()) ;
			}
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan) ;
			list = new ArrayList<Result>() ;
			for (Result rs : scanner) {
				list.add(rs) ;
			}
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	
	public List<Result> getRowsByOneKey(String tableName, String rowKeyLike ,String cols[]) {
		// TODO Auto-generated method stub
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName) ;
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			
			Scan scan = new Scan();
			for (int i = 0; i < cols.length; i++) {
				scan.addColumn("cf".getBytes(), cols[i].getBytes()) ;
			}
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan) ;
			list = new ArrayList<Result>() ;
			for (Result rs : scanner) {
				list.add(rs) ;
			}
		} catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	
	/**
	 * 范围查询
	 * @param tableName
	 * @param startRow
	 * @param stopRow
	 * @return
	 */
	public List<Result> getRows(String tableName,String startRow,String stopRow)
	{
		HTableInterface table = null;
		List<Result> list = null;
		try {
			table = hTablePool.getTable(tableName) ;
			Scan scan = new Scan() ;
			scan.setStartRow(startRow.getBytes()) ;
			scan.setStopRow(stopRow.getBytes()) ;
			ResultScanner scanner = table.getScanner(scan) ;
			list = new ArrayList<Result>() ;
			for (Result rsResult : scanner) {
				list.add(rsResult) ;
			}
			
		}catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		return list;
	}
	
	
	public void deleteRecords(String tableName, String rowKeyLike){
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			PrefixFilter filter = new PrefixFilter(rowKeyLike.getBytes());
			Scan scan = new Scan();
			scan.setFilter(filter);
			ResultScanner scanner = table.getScanner(scan) ;
			List<Delete> list = new ArrayList<Delete>() ;
			for (Result rs : scanner) {
				Delete del = new Delete(rs.getRow());
				list.add(del) ;
			}
			table.delete(list);
		}
		catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public void deleteCell(String tableName, String rowkey,String cf,String column){
		HTableInterface table = null;
		try {
			table = hTablePool.getTable(tableName) ;
			Delete del = new Delete(rowkey.getBytes());
			del.deleteColumn(cf.getBytes(), column.getBytes());
			table.delete(del);
		}
		catch (Exception e) {
			e.printStackTrace() ;
		}
		finally
		{
			try {
				table.close() ;
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		
	}
	
	public   void createTable(String tableName, String[] columnFamilys){
		try {
			// admin 对象
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {
				System.err.println("此表，已存在！");
			} else {
				HTableDescriptor tableDesc = new HTableDescriptor(
						TableName.valueOf(tableName));

				for (String columnFamily : columnFamilys) {
					tableDesc.addFamily(new HColumnDescriptor(columnFamily));
				}

				admin.createTable(tableDesc);
				System.err.println("建表成功!");

			}
			admin.close();// 关闭释放资源
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * 删除一个表
	 * 
	 * @param tableName
	 *            删除的表名
	 * */
	public   void deleteTable(String tableName)   {
		try {
			HBaseAdmin admin = new HBaseAdmin(conf);
			if (admin.tableExists(tableName)) {
				admin.disableTable(tableName);// 禁用表
				admin.deleteTable(tableName);// 删除表
				System.err.println("删除表成功!");
			} else {
				System.err.println("删除的表不存在！");
			}
			admin.close();
		} catch (MasterNotRunningException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
 
	/**
	 * 查询表中所有行
	 * @param tablename
	*/
	public void scaner(String tablename) {
	try {
	        HTable table =new HTable(conf, tablename);
	        Scan s =new Scan();
//	        s.addColumn(family, qualifier)
//	        s.addColumn(family, qualifier)
	        ResultScanner rs = table.getScanner(s);
	for (Result r : rs) {
	            
		   for(Cell cell:r.rawCells()){   
			    System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
			    System.out.println("Timetamp:"+cell.getTimestamp()+" ");
			    System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
			    System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
			    System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
			   }
	        }
	    } catch (IOException e) {
	        e.printStackTrace();
	    }
	}
	public void scanerByColumn(String tablename) {
		
		try {
			HTable table =new HTable(conf, tablename);
			Scan s =new Scan();
	        s.addColumn("cf".getBytes(), "201504052237".getBytes());
	        s.addColumn("cf".getBytes(), "201504052237".getBytes());
			ResultScanner rs = table.getScanner(s);
			for (Result r : rs) {
				
				for(Cell cell:r.rawCells()){   
					System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
					System.out.println("Timetamp:"+cell.getTimestamp()+" ");
					System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
					System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
					System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	public static void main(String[] args) {
		

		
		
//		创建表
//	    String tableName="test";
//		String cfs[] = {"cf"};
//		dao.createTable(tableName,cfs);
		
//		存入一条数据
//		Put put = new Put("bjsxt".getBytes());
//		put.add("cf".getBytes(), "name".getBytes(), "cai10".getBytes()) ;
//		dao.save(put, "test") ;

//		插入多列数据
//		Put put = new Put("bjsxt".getBytes());
//		List<Put> list = new ArrayList<Put>();
//		put.add("cf".getBytes(), "addr".getBytes(), "shanghai1".getBytes()) ;
//		put.add("cf".getBytes(), "age".getBytes(), "30".getBytes()) ;
//		put.add("cf".getBytes(), "tel".getBytes(), "13889891818".getBytes()) ;
//		list.add(put) ;
//		dao.save(list, "test");
		
//		插入单行数据
//		dao.insert("test", "testrow", "cf", "age", "35") ;
//		dao.insert("test", "testrow", "cf", "cardid", "12312312335") ;
//		dao.insert("test", "testrow", "cf", "tel", "13512312345") ;
		
		
		
//		List<Result> list = dao.getRows("test", "testrow",new String[]{"age"}) ;
//		for(Result rs : list)
//		{
//		    for(Cell cell:rs.rawCells()){   
//		        System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
//		        System.out.println("Timetamp:"+cell.getTimestamp()+" ");
//		        System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
//		        System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
//		        System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
//		       }
//		}
		
//		Result rs = dao.getOneRow("test", "testrow");
//		 System.out.println(new String(rs.getValue("cf".getBytes(), "age".getBytes())));
		
//		Result rs = dao.getOneRowAndMultiColumn("cell_monitor_table", "29448-513332015-04-05", new String[]{"201504052236","201504052237"});
//		for(Cell cell:rs.rawCells()){   
//	        System.out.println("RowName:"+new String(CellUtil.cloneRow(cell))+" ");
//	        System.out.println("Timetamp:"+cell.getTimestamp()+" ");
//	        System.out.println("column Family:"+new String(CellUtil.cloneFamily(cell))+" ");
//	        System.out.println("row Name:"+new String(CellUtil.cloneQualifier(cell))+" ");
//	        System.out.println("value:"+new String(CellUtil.cloneValue(cell))+" ");
//	       }
		
//		dao.deleteTable("cell_monitor_table");
//		创建表
	    String tableName="cell_monitor_table";
		String cfs[] = {"cf"};
//		dao.createTable(tableName,cfs);
	}
	
	
	  public static void testRowFilter(String tableName){
	     	try {
	     		 HTable table =new HTable(conf, tableName);
				Scan scan = new Scan();
				scan.addColumn(Bytes.toBytes("column1"),Bytes.toBytes("qqqq"));
				Filter filter1 = new RowFilter(CompareOp.LESS_OR_EQUAL,new BinaryComparator(Bytes.toBytes("laoxia157")));
				scan.setFilter(filter1);
				ResultScanner scanner1 = table.getScanner(scan);
				for (Result res : scanner1) {
				    System.out.println(res);
				}
				scanner1.close();

//			 
//				Filter filter2 = new RowFilter(CompareFilter.CompareOp.EQUAL,new RegexStringComparator("laoxia4\\d{2}"));
//				scan.setFilter(filter2);
//				ResultScanner scanner2 = table.getScanner(scan);
//				for (Result res : scanner2) {
//				     System.out.println(res);
//				}
//				scanner2.close();

				Filter filter3 = new RowFilter( CompareOp.EQUAL,new SubstringComparator("laoxia407"));
				scan.setFilter(filter3);
				ResultScanner scanner3 = table.getScanner(scan);
				for (Result res : scanner3) {
				      System.out.println(res);
				}
				scanner3.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
	    }
	    
	  @Test
	  public void testTrasaction(){
		  try{
			  HTableInterface table = null;
			  table = hTablePool.getTable("t_test".getBytes());
//			  Put put1 =new Put("002".getBytes());
//			  put1.add("cf1".getBytes(), "name".getBytes(), "王五".getBytes());
//			  table.put(put1);
			  Put newput =new Put("001".getBytes());
			  newput.add("cf1".getBytes(), "like".getBytes(), "看书".getBytes());
			  
			  boolean f= table.checkAndPut("001".getBytes(), "cf1".getBytes(), "age".getBytes(), "24".getBytes(), newput);
			  System.out.println(f);
			  
		  }catch (Exception e){
			  e.printStackTrace();
		  }
		  
	  }
}
