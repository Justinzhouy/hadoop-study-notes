package com.zhouy.test.sequence.file;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.SequenceFile.Reader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.util.ReflectionUtils;

public class SequenceFileReaderDemo {
	
	public static void main(String[] args) {
		String uri = "hdfs://master.hadoop:9999/sequence/write/comfile.txt";
		Configuration conf = new Configuration();
		Reader reader = null;
		try {
			reader = new Reader(conf, Reader.file(new Path(uri)));
			Writable key = (Writable) ReflectionUtils.newInstance(reader.getKeyClass(), conf);
			Writable value = (Writable) ReflectionUtils.newInstance(reader.getValueClass(), conf);
			long position = reader.getPosition();
			while(reader.next(key, value)) {
				System.out.println(position + "=======" + key + "======" + value);
				position = reader.getPosition();
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(reader);
		}
	}
}
