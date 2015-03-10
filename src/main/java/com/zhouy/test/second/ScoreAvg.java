package com.zhouy.test.second;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class ScoreAvg {
	
	private static class ScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable>{
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] arr = line.split("\\s+");
			System.out.println(line);
			System.out.println(arr[0]);
			context.write(new Text(arr[0]), new IntWritable(Integer.parseInt(arr[1])));
		}
	}
	
	private static class ScoreReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
		@Override
		protected void reduce(Text arg0, Iterable<IntWritable> arg1,
				Reducer<Text, IntWritable, Text, IntWritable>.Context context)
				throws IOException, InterruptedException {
			int sum = 0;
			int count = 0;
			Iterator<IntWritable> iterator = arg1.iterator();
			while(iterator.hasNext()) {
				sum += iterator.next().get();
				count++;
			}
			context.write(arg0, new IntWritable(sum / count));
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJarByClass(ScoreAvg.class);
		job.setJobName("Score avg job");
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(ScoreMapper.class);
		//job.setCombinerClass(ScoreReduce.class);
		job.setReducerClass(ScoreReduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://master.hadoop:9999/inputscore1"));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master.hadoop:9999/outputscore1"));
		
		System.out.println(job.waitForCompletion(true));
	}
}
