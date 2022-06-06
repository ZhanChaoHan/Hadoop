package com.jachs.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

/***
 * 
 * @author zhanchaohan
 *
 */
public class WordCount {
	/*输入文件地址，测试内容如下：
	 * ------------------------
	  Hello Workd Bye World
	  Hello Hadoop Goodbye Hadoop
	  Hello Workd Bye World And Hello Hadoop Goodbye Hadoop
	 ------------------------
	  */
	private static final String InputFile="/usr/jachs/hadoop/A";
	
	//输出路径
	private static final String OutDir="/usr/jachs/hadoop/B";
	
	//将读取文件的内容切割出每个单词，标记数量为1,<word,1>形式，然后交给Reduce处理
	public static class Map extends MapReduceBase implements Mapper<LongWritable,Text,Text,IntWritable>{
		private final static IntWritable one=new IntWritable(1);
		private Text word=new Text();
		
		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException {
			String line=value.toString();
			StringTokenizer token=new StringTokenizer(line);
			
			while(token.hasMoreTokens()) {
				word.set(token.nextToken());
				output.collect(value, one);
			}
			
		}
	}
	//Reduce简单将数值累计求和
	public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable>{
		public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output,
				Reporter reporter) throws IOException {
			int sum=0;
			while (values.hasNext()) {
				sum+=values.next().get();
			}
			output.collect(key, new IntWritable(sum));
		}
	}
	
	public static void main(String[] args) throws IOException {
		JobConf conf=new JobConf(WordCount.class);
		conf.setJobName("wordCount");
		
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
		
		conf.setMapperClass(Map.class);
		conf.setReducerClass(Reduce.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(conf, new Path(InputFile));
		FileOutputFormat.setOutputPath(conf, new Path(OutDir));
		
		JobClient.runJob(conf);
	}
}
