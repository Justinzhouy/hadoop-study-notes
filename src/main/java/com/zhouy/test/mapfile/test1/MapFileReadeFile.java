package com.zhouy.test.mapfile.test1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.MapFile.Reader;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.util.ReflectionUtils;

public class MapFileReadeFile {
	
	public static void main(String[] args) {
		String uri = "hdfs://master.hadoop:9999/myfile/write";
		Configuration conf = new Configuration();
		Reader reader = null;
		try {
			reader = new Reader(new Path(uri), conf);
			WritableComparable key = (WritableComparable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			WritableComparable val = (WritableComparable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			while(reader.next(key, val)) {
				System.out.println(key + "==========" + val);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}
