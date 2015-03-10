package com.zhouy.test.forth;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Sort {

	private static class Map extends Mapper<LongWritable, Text, IntWritable, IntWritable> {
		@Override
		protected void map(LongWritable key, Text value, Mapper<LongWritable, Text, IntWritable, IntWritable>.Context context)
				throws IOException, InterruptedException {
			System.out.println(value.toString());
			context.write(new IntWritable(Integer.valueOf(value.toString())), new IntWritable(1));
		}
	}
	
	private static class Reduce extends Reducer<IntWritable, IntWritable, IntWritable, IntWritable> {
		private static IntWritable index = new IntWritable(1);
		@Override
		protected void reduce(IntWritable arg0, Iterable<IntWritable> arg1, Reducer<IntWritable, IntWritable, IntWritable, IntWritable>.Context arg2)
				throws IOException, InterruptedException {
			System.out.println(arg0);
			arg2.write(index, arg0);
			index = new IntWritable(index.get() + 1);
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(Sort.class);
		job.setJobName("sort job");
		
		job.setOutputKeyClass(IntWritable.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
//		job.setCombinerClass(Reduce.class);
		job.setReducerClass(Reduce.class);
		
//		job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://master.hadoop:9999" + args[0]));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master.hadoop:9999" + args[1]));
		
		System.out.println(job.waitForCompletion(true));
	}

}
