package com.zhouy.test.thrid;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Unique {

	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		private final Text TEXT_ENPTY = new Text("");
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			context.write(value, TEXT_ENPTY);
		}
	}
	
	private static class Reduce extends Reducer<Text, Text, Text, Text> {
		private final Text TEXT_ENPTY = new Text("");
		@Override
		protected void reduce(Text arg0, Iterable<Text> arg1,
				Reducer<Text, Text, Text, Text>.Context arg2)
				throws IOException, InterruptedException {
			arg2.write(arg0, TEXT_ENPTY);
		}
	}
	
	public static void main(String[] args) throws Exception{
		Job job = Job.getInstance();
		
		job.setJarByClass(Unique.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://master.hadoop:9999" + args[0]));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master.hadoop:9999" + args[1]));
		
		System.out.println(job.waitForCompletion(true));
	}
}
