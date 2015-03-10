package com.zhouy.test.first;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCountThree
{
	public static class Map extends Mapper<LongWritable, Text, Text, IntWritable>
	{
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();
		
		public void map(LongWritable key, Text value, Context context)
		{
			StringTokenizer tokenizer = new StringTokenizer(value.toString());
			while (tokenizer.hasMoreTokens())
			{
				word.set(tokenizer.nextToken());
				try
				{
					context.write(word, one);
				}
				catch (Exception e)
				{
					e.printStackTrace();
				}
			}
		}
	}
	
	public static class Reduce extends Reducer<Text, IntWritable, Text, IntWritable>
	{
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context)
		{
			int sum = 0;
			for (IntWritable val : values)
			{
				sum += val.get();
			}
			result.set(sum);
			try
			{
				context.write(key, result);
			}
			catch (Exception e)
			{
				e.printStackTrace();
			}
		}
	}
	
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		
		Job job = null;
		try
		{
			job = Job.getInstance(conf);
		}
		catch (IOException e)
		{
			e.printStackTrace();
		}
		
		job.setJarByClass(WordCountThree.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path("hdfs://master.hadoop:9999/input"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master.hadoop:9999/output"));
		System.out.println(job.waitForCompletion(true));
	}
}
