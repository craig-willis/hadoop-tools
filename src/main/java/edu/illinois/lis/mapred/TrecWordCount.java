package edu.illinois.lis.mapred;

import java.io.*;




import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;
import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.util.Version;

import edu.illinois.hadoop.trec.TrecDocumentInputFormat;
import edu.illinois.lis.hadoop.TrecUtils;
import edu.umd.cloud9.collection.trec.TrecDocument;

/**
 * DocumentWordCount:  Counts the number of documents each 
 * word occurs in for a collection.
 */
public class TrecWordCount extends Configured implements Tool 
{
	// Counter to track the number of documents and terms in the input file
	public static enum Count { DOCS  };

	private static IntWritable one = new IntWritable(1);
	

	/**
	 * Map implementation: tokenize the input text and collect
	 * the unique set of words for each document.
	 */
	public static class TrecWordCountMapper extends MapReduceBase 
  		implements Mapper<LongWritable, TrecDocument, Text, IntWritable> 
	{
		Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_43);
		Text term = new Text();

		long maxDocNo = -1;
		
	    public void configure(JobConf job) 
	    {               
	        maxDocNo = Long.parseLong(job.get("maxDocNo"));
	    }
	    
		public void map(LongWritable key, TrecDocument doc, 
				OutputCollector<Text, IntWritable> output, Reporter reporter) 
						throws IOException 
		{
			reporter.getCounter(Count.DOCS).increment(1);

	        TokenStream stream = analyzer.tokenStream(null,
	                new StringReader(TrecUtils.getText(doc)));
	        stream.reset();

	        // Only process documents earlier than the specified docno
	        String docno = TrecUtils.getDocNo(doc);
	        if (maxDocNo > 0 && Long.parseLong(docno) >= maxDocNo)
	            return;
	        
	        //stream = new EnglishPossessiveFilter(Version.LUCENE_43, stream);
	        CharTermAttribute cattr = stream.addAttribute(CharTermAttribute.class);

	        Set<String> words = new HashSet<String>();
			while (stream.incrementToken()) {
				words.add(cattr.toString());
			}
			
	    	for (String word: words) {
		        term.set(word);
		        output.collect(term, one);
	    	}	    	
		}
	}

	
	/**
	 * Reducer implementation: 
	 * @author cwillis
	 *
	 */
	public static class TrecWordCountReducer extends MapReduceBase 
		implements Reducer<Text, IntWritable, Text, LongWritable> 
	{		
		private LongWritable sum = new LongWritable();
		private int MIN_OCCUR = 5;
		
		
		/**
		 * Sum up the document occurrences for a word.  Only collect those
		 * that occur more than MIN_OCCUR times and less than MAX_OCCUR_PCT times.
		 */
		public void reduce(Text word, Iterator<IntWritable> values, 
				OutputCollector<Text, LongWritable> output, Reporter reporter) 
						throws IOException 
		{
			int cnt = 0;
			while (values.hasNext()) {
				IntWritable val = values.next();
				cnt += val.get();
			}
			sum.set(cnt);
			
			if (cnt > MIN_OCCUR)
				output.collect(word,  sum);		
		}
	}
	
	public int run(String[] args) throws Exception 
	{
		JobConf conf = new JobConf(getConf(), TrecWordCount.class);
		conf.setJobName("word-count");
		
		conf.setMapperClass(TrecWordCountMapper.class);
		conf.setReducerClass(TrecWordCountReducer.class);
		
		conf.setInputFormat(TrecDocumentInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);
		
		conf.setMapOutputKeyClass(Text.class);
		conf.setMapOutputValueClass(IntWritable.class);
		  
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(IntWritable.class);
	
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		
		JobClient.runJob(conf);
		  
		return 0;
	}
	
	public static void main(String[] args) throws Exception {
		int res = ToolRunner.run(new Configuration(), new TrecWordCount(), args);
		System.exit(res);
	}
}

