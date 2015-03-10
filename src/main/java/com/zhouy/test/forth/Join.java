package com.zhouy.test.forth;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Join {
	
	private static class Map extends Mapper<LongWritable, Text, Text, Text> {
		@Override
		protected void map(LongWritable key, Text value,
				Mapper<LongWritable, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			String line = value.toString();
			String[] strArr = line.split("\\s+");
			context.write(new Text(strArr[1]), new Text("child-" + strArr[0]));
			context.write(new Text(strArr[0]), new Text("grand-" + strArr[1]));
		}
	}
	
	private static class Reduce extends Reducer<Text, Text, Text, Text> {
		@Override
		protected void reduce(Text key, Iterable<Text> value,
				Reducer<Text, Text, Text, Text>.Context context)
				throws IOException, InterruptedException {
			List<String> childs = new ArrayList<String>();
			List<String> grands = new ArrayList<String>();
			Iterator<Text> iterator = value.iterator();
			while(iterator.hasNext()) {
				String tmp = iterator.next().toString();
				if(tmp.startsWith("child-")) {
					childs.add(tmp.substring(6));
				} else if(tmp.startsWith("grand-")) {
					grands.add(tmp.substring(6));
				}
			}
			for (String child : childs) {
				for (String grand : grands) {
					context.write(new Text(child), new Text(grand));
				}
			}
		}
	}
	
	public static void main(String[] args) throws Exception {
		Job job = Job.getInstance();
		job.setJobName("join");
		job.setJarByClass(Join.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		FileInputFormat.setInputPaths(job, new Path("hdfs://master.hadoop:9999" + args[0]));
		FileOutputFormat.setOutputPath(job, new Path("hdfs://master.hadoop:9999" + args[1]));
		
		System.out.println(job.waitForCompletion(true));
	}
}
