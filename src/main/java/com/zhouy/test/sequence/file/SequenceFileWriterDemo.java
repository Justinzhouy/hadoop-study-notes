package com.zhouy.test.sequence.file;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.SequenceFile.Writer;
import org.apache.hadoop.io.Text;

public class SequenceFileWriterDemo {
	
	private static final String[] STRING_VALUES = {
		"HELLO WORLD",
		"BYB WORLD",
		"HELLO HODOOP",
		"BYB HADOOP"
	};
	
	public static void main(String[] args) throws IOException {
		String uri = "hdfs://master.hadoop:9999/sequence/write/unfile.txt";
		Configuration conf = new Configuration();
		Path path = new Path(uri);
		SequenceFile.Writer writer = null;
		IntWritable key = new IntWritable();
		Text value = new Text();
		try {
			writer = SequenceFile.createWriter(conf,
					Writer.file(path), Writer.keyClass(key.getClass()),
					Writer.valueClass(value.getClass()));
			for(int i = 0; i< 500000; i++) {
				key.set(500000 - i);
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
