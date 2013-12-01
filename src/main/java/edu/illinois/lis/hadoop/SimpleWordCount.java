package edu.illinois.lis.hadoop;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;


/**
 * Simple word count mapreduce job based on the example in the 
 * Hadoop documentation.
 */
public class SimpleWordCount extends Configured 
	implements Tool 
{
        
	/**
	 * Map implementation: tokenize the input line and 
	 * collect per word.
	 */
	public static class WordCountMap extends MapReduceBase 
		implements  Mapper<LongWritable, Text, Text, IntWritable> 
	{
	 
		private final static IntWritable one = new IntWritable(1);
		private Text word = new Text();   

		public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, 
			Reporter reporter) throws IOException 
		{
			String line = value.toString();
			StringTokenizer tokenizer = new StringTokenizer(line);
			while (tokenizer.hasMoreTokens()) {
				word.set(tokenizer.nextToken());
				output.collect(word, one);
			}	
		}
	} 
       
	/**
	 * Reduce implementation: Sum up the counts for each word
	 */
	public static class WordCountReduce extends MapReduceBase 
		implements Reducer<Text, IntWritable, Text, IntWritable> 
	{
	
		public void reduce(Text key, Iterator<IntWritable> values,
				OutputCollector<Text, IntWritable> output, Reporter reporter)
				throws IOException 
		{
	        int sum = 0;
	        
	        while (values.hasNext()) {
	        	IntWritable val = values.next();
	            sum += val.get();
	        }
	        output.collect(key, new IntWritable(sum));	
		}
	 }

	
	 public int run(String[] args) throws Exception {
	    
		  JobConf conf = new JobConf(getConf(), SimpleWordCount.class);
		  conf.setJobName("simple-wordcount");

		  conf.setMapperClass(WordCountMap.class);
		  conf.setReducerClass(WordCountReduce.class);

		  // Both mapper and reducer work with text and intwritable
		  conf.setOutputKeyClass(Text.class);
		  conf.setOutputValueClass(IntWritable.class);
		
		  conf.setInputFormat(TextInputFormat.class);
		  conf.setOutputFormat(TextOutputFormat.class);
		
		  FileInputFormat.setInputPaths(conf, new Path(args[0]));
		  FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		  JobClient.runJob(conf);
		    
		  return 0;
	 }
    
	 public static void main(String[] args) throws Exception {
		  int res = ToolRunner.run(new Configuration(), new SimpleWordCount(), args);
		  System.exit(res);
	 }
        
}
