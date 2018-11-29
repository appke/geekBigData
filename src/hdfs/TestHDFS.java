package com.sxt.hadoop.hdfs;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.HdfsBlockLocation;
import org.apache.hadoop.fs.Options;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.SequenceFile.Writer.Option;
import org.apache.hadoop.io.Text;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TestHDFS {
	
	Configuration  conf = null;
	FileSystem  fs = null;
	
	
	@Before
	public void conn() throws Exception{
		
		conf = new Configuration(false);
		conf.set("fs.defaultFS", "hdfs://node01:9000");
		fs = FileSystem.get(conf);
	}
	
	@After
	public void close() throws Exception{
		fs.close();
	}
	
	@Test
	public void testConf(){
		System.out.println(conf.get("fs.defaultFS"));
		
	}
	
	
	@Test
	public void mkdir() throws Exception{
		
		Path dir = new Path("/sxt");
		if(!fs.exists(dir)){
			fs.mkdirs(dir );
		}
	}
	
	
	@Test
	public void uploadFile() throws Exception{
		
		
		Path file = new Path("/sxt/ok.txt");
		FSDataOutputStream output = fs.create(file );
		
		InputStream input = new BufferedInputStream(new FileInputStream(new File("c:\\nginx")) ) ;
		
		IOUtils.copyBytes(input, output, conf, true);
		
	}
	
	@Test
	public void blk() throws Exception{
		
		
		Path file = new Path("/user/root/test.txt");
		FileStatus ffs = fs.getFileStatus(file );
		BlockLocation[] blks = fs.getFileBlockLocations(ffs , 0, ffs.getLen());
		
		for (BlockLocation b : blks) {
			
			System.out.println(b);
			HdfsBlockLocation hbl = (HdfsBlockLocation)b;
			System.out.println(hbl.getLocatedBlock().getBlock().getBlockId());
		}
		
		FSDataInputStream input = fs.open(file);
		
		System.out.println((char)input.readByte());
		
		input.seek(1048576);
		
		System.out.println((char)input.readByte());
		
	}
	
	
	@Test
	public void seqfile() throws Exception{
		
		
		Path value = new Path("/haha.seq");
		
		IntWritable key = new IntWritable();
		Text val = new Text();
		Option file = SequenceFile.Writer.file(value );
		Option keyClass = SequenceFile.Writer.keyClass(key.getClass());
		Option valueClass = SequenceFile.Writer.valueClass(val.getClass());
		
		Writer writer = SequenceFile.createWriter(conf, file,keyClass,valueClass);
		
		for (int i = 0; i < 10; i++) {
			key.set(i);
			val.set("sxt..."+i);
			writer.append(key, val);
			
		}
		writer.hflush();
		writer.close();
		
		
		SequenceFile.Reader.Option infile = Reader.file(value);
		SequenceFile.Reader reader = new SequenceFile.Reader(conf,infile);
		
		String name = reader.getKeyClassName();
		System.out.println(name);
		
		
		
		
		
	}
	
	
	
	
	
	
	
	
	
	
	

}
