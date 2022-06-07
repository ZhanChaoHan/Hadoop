package com.jachs.hadoop;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configured;
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
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/***
 * 
 * @author zhanchaohan
 *
 */
public class WordCount extends Configured implements Tool{
	private static final String InputFile="/usr/jachs/hadoop/A";
	//输出路径
	private static final String OutDir="/usr/jachs/hadoop/B";
	
	public static class Map extends Mapper<LongWritable,Text,Text,IntWritable>{
		private final static IntWritable one=new IntWritable(1);
		private Text word=new Text();
		
		public void map(LongWritable key,Text value,Context context) throws IOException,InterruptedException{
			String line=value.toString();
			StringTokenizer token=new StringTokenizer(line);
			while(token.hasMoreTokens()) {
				word.set(token.nextToken());
				context.write(word, one);
			}
		}
	}
	
	public static class Reduce extends Reducer<Text,IntWritable,Text,IntWritable>{
		public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum=0;
			for (IntWritable val : values) {
				sum+=val.get();
			}
			context.write(key, new IntWritable(sum));
		}
	}
	public int run(String[] arg0) throws Exception {
		Job job=new Job(getConf());
		job.setJarByClass(WordCount.class);
		job.setJobName("wordcount");
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		job.setMapperClass(Map.class);
		job.setReducerClass(Reduce.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(InputFile));
		FileOutputFormat.setOutputPath(job, new Path(OutDir));
		
		boolean success=job.waitForCompletion(true);
		return success ? 0:1;
	}

	public static void main(String[] args) throws Exception {
		int ret=ToolRunner.run(new WordCount(),args);
		System.exit(ret);
	}
}
