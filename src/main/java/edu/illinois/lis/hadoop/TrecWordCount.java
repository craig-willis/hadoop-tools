package edu.illinois.lis.hadoop;

import java.io.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import edu.umd.cloud9.collection.trec.TrecDocument;
import edu.umd.cloud9.collection.trec.TrecDocumentInputFormat;

/**
 * Count total word frequencies in a TREC document collection
 */
public class TrecWordCount extends Configured implements Tool 
{
	public static enum Count { DOCS };

	private static IntWritable one = new IntWritable(1);

	public static class TrecWordCountMapper extends Mapper <LongWritable, TrecDocument, Text, IntWritable> 
	{
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_43);
		Text term = new Text();

		public void map(LongWritable key, TrecDocument doc, Context context) 
						throws IOException, InterruptedException
		{			
			context.getCounter(Count.DOCS).increment(1);

	        TokenStream stream = analyzer.tokenStream(null,
	                new StringReader(getText(doc)));
	        stream.reset();

	        //stream = new EnglishPossessiveFilter(Version.LUCENE_43, stream);
	        CharTermAttribute cattr = stream.addAttribute(CharTermAttribute.class);

			while (stream.incrementToken())
			{
		        term.set(cattr.toString());
		        context.write(term, one);
			}
		}
		
		private static String getText(TrecDocument doc) {

			String text = "";
			String content = doc.getContent();
			int start = content.indexOf("<TEXT>");
			if (start == -1) {
				text = "";
			} else {
				int end = content.indexOf("</TEXT>", start);
				text= content.substring(start + 6, end).trim();
			}
			return text;
		}
	}
	
	public static class TrecWordCountReducer extends Reducer <Text, IntWritable, Text, LongWritable> 
	{		
		private LongWritable sum = new LongWritable();

	    public void reduce(Text key, Iterable<IntWritable> values, Context context)
	            throws IOException, InterruptedException 
	    {
			int cnt = 0;
			for (IntWritable val : values) {
				cnt += val.get();
			} 
			sum.set(cnt);
			context.write(key, sum);
	    }
	}
	
	public int run(String[] args) throws Exception 
	{
		Job job = Job.getInstance(getConf());
		job.setJarByClass(TrecWordCount.class);
		job.setInputFormatClass(TrecDocumentInputFormat.class);
		job.setMapperClass(TrecWordCountMapper.class);
		job.setReducerClass(TrecWordCountReducer.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		job.waitForCompletion(true);
		
		return 0;	
	}
	
	public static void main(String[] args) throws Exception 
	{
		ToolRunner.run(new Configuration(), new TrecWordCount(), args);
	}
}

