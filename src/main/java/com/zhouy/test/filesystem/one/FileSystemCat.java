package com.zhouy.test.filesystem.one;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.junit.Test;

public class FileSystemCat {
	
	private static FileSystem FILE_SYSTEM = null;
	
	static {
		Configuration conf = new Configuration();
		try {
			FILE_SYSTEM = FileSystem.get(URI.create("hdfs://master.hadoop:9999/"), conf);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args) throws Exception {
	//	String uri = "hdfs://master.hadoop:9999/filesystem/file.txt";
	//	FileSystemCat.mkdirs(uri);
	
	//	String uri = "hdfs://master.hadoop:9999/filesystem";
	//	FileSystemCat.delete(uri);

	    
	    //	String uri = "hdfs://master.hadoop:9999/input/file01";
	//	FileSystemCat.fileStatus(uri);
		String uri = "/input";
		FileSystemCat.listStatus(uri);
	}
	
	private static void test1(String uri) throws Exception {
		FileSystem fileSystem = FileSystem.get(URI.create(uri), new Configuration());
		InputStream in = null;
		try {
			in = fileSystem.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			in.close();
		}
	}
	
	private static void test2(String uri) throws Exception {
		FileSystem fileSystem = FileSystem.get(URI.create(uri), new Configuration());
		FSDataInputStream in = null;
		try {
			in = fileSystem.open(new Path(uri));
			IOUtils.copyBytes(in, System.out, 4096, false);
			System.out.println("=======================");
			in.seek(3l);
			IOUtils.copyBytes(in, System.out, 4096, false);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			in.close();
		}
	}
	
	private static void mkdirs(String uri) throws Exception {
		Configuration conf = new Configuration();
		//追加配置文件
		conf.set("dfs.support.append", "true");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.enable", "true");
		conf.set("dfs.client.block.write.replace-datanode-on-failure.policy", "NEVER");
		FileSystem fileSystem = FileSystem.get(URI.create(uri), conf);
		FileInputStream in = new FileInputStream("/Users/sunxiaochen/Desktop/memcache.xml");
		FSDataOutputStream out = fileSystem.append(new Path(uri + "test.txt"));
		IOUtils.copyBytes(in, out, conf, true);
	}
	
	private static void delete(String uri) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create(uri), conf);
		System.out.println(fileSystem.delete(new Path(uri), true));
	}
	
	private static void fileStatus(String uri) throws Exception {
		Configuration conf = new Configuration();
		FileSystem fileSystem = FileSystem.get(URI.create("hdfs://master.hadoop:9999/"), conf);
		FileStatus fileStatus = fileSystem.getFileStatus(new Path("/inputjoin/file"));
		FileSystemCat.showFileStatus(fileStatus);
	}
	
	private static void showFileStatus(FileStatus fileStatus) {
		SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		System.out.println(format.format(new Date(fileStatus.getAccessTime())));
		System.out.println(fileStatus.getBlockSize());
		System.out.println(fileStatus.getGroup());
		System.out.println(fileStatus.getLen());
		System.out.println(format.format(new Date(fileStatus.getModificationTime())));
		System.out.println(fileStatus.getOwner());
		System.out.println(fileStatus.getPath());
		System.out.println(fileStatus.getPermission().toString());
		System.out.println(fileStatus.getReplication());
	}
	
	private static void listStatus(String uri) throws Exception {
		Path path = new Path(uri);
		FileStatus fileStatus = FILE_SYSTEM.getFileStatus(path);
		if(fileStatus.isDirectory()) {
			FileStatus[] list = FILE_SYSTEM.listStatus(path);
			int index = 1;
			for (FileStatus tmpStatus : list) {
				System.out.println(index++ + "===================");
				FileSystemCat.showFileStatus(tmpStatus);
			}
		}else {
			FileSystemCat.showFileStatus(fileStatus);
		}
	}
	
	@Test
	public void listPathPattern() throws Exception {
		FileStatus[] list = FILE_SYSTEM.globStatus(new Path("/input*"));
		int index = 1;
		for (FileStatus tmpStatus : list) {
			System.out.println(index++ + "===================");
			FileSystemCat.showFileStatus(tmpStatus);
		}
	}
}
