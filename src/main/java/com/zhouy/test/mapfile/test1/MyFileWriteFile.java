package com.zhouy.test.mapfile.test1;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.MapFile.Writer;
import org.apache.hadoop.io.Text;

public class MyFileWriteFile {
	
	private static final String[] STRING_VALUES = {
		"HELLO WORLD",
		"BYB WORLD",
		"HELLO HODOOP",
		"BYB HADOOP"
	};
	
	public static void main(String[] args) {
		String uri = "hdfs://master.hadoop:9999/myfile/write";
		Configuration conf = new Configuration();
		Path path = new Path(uri);
		Writer writer = null;
		IntWritable key = new IntWritable();
		Text value = new Text();
		try {
			writer = new Writer(conf, path, Writer.keyClass(key.getClass()), Writer.valueClass(value.getClass()));
			for(int i = 0; i< 500; i++) {
				key.set(i);
				value.set(STRING_VALUES[i%STRING_VALUES.length]);
				writer.append(key, value);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			IOUtils.closeStream(writer);
		}
	}
}
