package edu.illinois.lis.mapred;

import java.io.*;


import java.util.*;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.*;
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
import edu.illinois.lis.mapred.TrecWordCount.TrecWordCountMapper;
import edu.illinois.lis.mapred.TrecWordCount.TrecWordCountReducer;
import edu.umd.cloud9.collection.trec.TrecDocument;


public class TrecMutualInfoPair extends Configured  implements Tool 
{
    public static enum Count { DOCS  };	

	/**
	 * Mapper implementation:  Tokenize each input string and 
	 * collect all of the unique words.  For each word, collect
	 * an associative array (MapWritable) of all other co-occurring
	 * words.
	 */
	public static class TrecMutualInfoMapper extends MapReduceBase 
	    implements Mapper<LongWritable, TrecDocument, Text, IntWritable> 
	{
        Map<String, Integer> documentFreq = new HashMap<String, Integer>();
	    Set<String> wordList = new HashSet<String>();
	    Text term = new Text();
	    IntWritable one = new IntWritable(1);
	    int docs = 0;
        long maxDocNo = -1;

	      
        public void configure(JobConf job) 
        {   
            
            try {
                maxDocNo = Long.parseLong(job.get("maxDocNo"));

                // Read the DocumentWordCount output file
                Path[] files = DistributedCache.getLocalCacheFiles(job);
                for (Path file: files) {
                    if (file.toString().contains("entities")) {
                        List<String> words = FileUtils.readLines(new File(file.toString()));
                        wordList.addAll(words);
                        System.out.println("Read " + wordList.size() + " words from wordList " + file.toString());
                    }
                    else
                    {
                        System.out.println("Reading total word counts from: " + file.toString());
                        List<String> lines = FileUtils.readLines(new File(file.toString()));
                        for (String line: lines) {
                            String[] fields = line.split("\t");
                            documentFreq.put(fields[0], Integer.parseInt(fields[1]));
                        }
                        System.out.println("Read " + documentFreq.size() + " words");
                    }
                }
            } catch (IOException ioe) {
                System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
            }               
	    }
       
		public void map(LongWritable key, TrecDocument doc, 
				OutputCollector<Text, IntWritable> output, Reporter reporter) 
						throws IOException 
		{	    	
		    
		    String docno = TrecUtils.getDocNo(doc);
		    System.out.println("Starting doc: " + docno + "(" + (docs++) + ")");
		    
	        // Only process documents earlier than the specified docno
            if (maxDocNo > 0 && Long.parseLong(docno) >= maxDocNo)
                return;
            
		    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_43);
		    
		    IntWritable one = new IntWritable(1);
		        
		    reporter.getCounter(Count.DOCS).increment(1);

	        TokenStream stream = analyzer.tokenStream(null,
	                    new StringReader(TrecUtils.getText(doc)));
	        stream.reset();
	        
	        //stream = new EnglishPossessiveFilter(Version.LUCENE_43, stream);
	        CharTermAttribute cattr = stream.addAttribute(CharTermAttribute.class);
	            
	         
	        Set<String> words = new HashSet<String>();
			while (stream.incrementToken()) {
				if(!cattr.toString().matches("\\d+(\\.\\d+)?")) {
					words.add(cattr.toString());
				}
			}
			
			for (String word1: words) 
			{
			    if ((wordList.size() > 0) && (!wordList.contains(word1)))
			        continue;
			    
				for (String word2: words) 
				{	
					if (word1.equals(word2))
						continue;
	                
	                // Only consider words from the word count phase					
	                if (documentFreq.containsKey(word1) && 
                            documentFreq.containsKey(word2))
                    {
    					
	                    term.set(word1 + "\t" + word2);
    					output.collect(term, one);
                    }
				}
			}
			analyzer.close();
		}
	}
	
	/**
	 * Reducer implementation: Reads the total number of documents (N) from 
	 * input parameter, reads total document-word counts from output of 
	 * DocumentWordCount MR process.
	 *
	 */
	public static class TrecMutualInfoReducer extends MapReduceBase 
		implements Reducer<Text, IntWritable, Text, DoubleWritable> 
	{
		
		Map<String, Integer> documentFreq = new HashMap<String, Integer>();
		long totalNumDocs = 0;
		public void configure(JobConf job) 
		{			    
			totalNumDocs = Long.parseLong(job.get("numDocs"));

			try {
				// Read the DocumentWordCount output file
				Path[] files = DistributedCache.getLocalCacheFiles(job);
				for (Path file: files) {
                    if (file.toString().contains("entities"))
                        continue;
					System.out.println("Reading total word counts from: " + file.toString());
					List<String> lines = FileUtils.readLines(new File(file.toString()));
					for (String line: lines) {
						String[] fields = line.split("\t");
						documentFreq.put(fields[0], Integer.parseInt(fields[1]));
					}
					System.out.println("Read " + documentFreq.size() + " words");
				}
			} catch (IOException ioe) {
				System.err.println("Caught exception while getting cached files: " + StringUtils.stringifyException(ioe));
			}
		
		}

        DoubleWritable mutualInfo = new DoubleWritable();
        
		public void reduce(Text termPair, Iterator<IntWritable> values, 
				OutputCollector<Text, DoubleWritable> output, Reporter reporter) 
						throws IOException 
		{ 
	        String[] terms = termPair.toString().split("\\t");
	        long jointOccur = 0;
	        
	        while (values.hasNext()) {
	           IntWritable val = values.next();
	           jointOccur += val.get();
	        }
	        
			//        | wordY | ~wordY |
			// -------|-------|--------|------
			//  wordX | nX1Y1 | nX1Y0  | nX1
			// ~wordX | nX0Y1 | nX0Y0  | nX0
			// -------|-------|--------|------
			//        |  nY1  |  nY0   | total

			double nX1Y1 = jointOccur;
			double nX1 = documentFreq.get(terms[0]);
			double nY1 = documentFreq.get(terms[1]);

			double emim = calculatePmi(totalNumDocs, nX1Y1, nX1, nY1);
			
			mutualInfo.set(emim);
			output.collect(termPair, mutualInfo);
		}
		
		private double calculatePmi(double N, double nX1Y1, double nX1, double nY1)
		{
			
			//        | wordY | ~wordY |
			// -------|-------|--------|------
			//  wordX | nX1Y1 | nX1Y0  | nX1
			// ~wordX | nX0Y1 | nX0Y0  | nX0
			// -------|-------|--------|------
			//        |  nY1  |  nY0   | gt

			// Marginal and joint frequencies
			double nX0 = N - nX1;
			double nY0 = N - nY1;		
			double nX1Y0 = nX1 - nX1Y1;
			double nX0Y1 = nY1 - nX1Y1;
			double nX0Y0 = nX0 - nX0Y1;

			// Marginal probabilities (smoothed)
			double pX1 = (nX1 + 0.5)/(1+N);
			double pX0 = (nX0 + 0.5)/(1+N);			
			double pY1 = (nY1 + 0.5)/(1+N);
			double pY0 = (nY0 + 0.5)/(1+N);
			
			// Joint probabilities (smoothed)
			double pX1Y1 = (nX1Y1 + 0.25) / (1+N);
			double pX1Y0 = (nX1Y0 + 0.25) / (1+N);
			double pX0Y1 = (nX0Y1 + 0.25) / (1+N);
			double pX0Y0 = (nX0Y0 + 0.25) / (1+N);
			
			// 
			double emim =  
					pX1Y1 * log2(pX1Y1, pX1*pY1) + 
					pX1Y0 * log2(pX1Y0, pX1*pY0) +
					pX0Y1 * log2(pX0Y1, pX0*pY1) +
					pX0Y0 * log2(pX0Y0, pX0*pY0);
			
			return emim;
		}
		

	}
	
	private static double log2(double num, double denom) {
		if (num == 0 || denom == 0)
			return 0;
		else
			return Math.log(num/denom)/Math.log(2);
	}

	
	  public int run(String[] args) throws Exception 
	  {
		  Path inputPath = new Path(args[0]);
		  Path wcOutputPath = new Path(args[1]);
		  Path miOutputPath = new Path(args[2]);
          Path wordList = new Path(args[3]);
          int numReducers = Integer.parseInt(args[4]);
          long maxDocNo = Long.valueOf(args[5]);
		  

		  JobConf wc = new JobConf(getConf(), TrecWordCount.class);
		  wc.setJobName("trec-word-count");
          wc.set("maxDocNo", String.valueOf(maxDocNo));
          wc.setNumReduceTasks(numReducers);

		  wc.setMapperClass(TrecWordCountMapper.class);
		  wc.setReducerClass(TrecWordCountReducer.class);
			
		  wc.setInputFormat(TrecDocumentInputFormat.class);
		  wc.setOutputFormat(TextOutputFormat.class);
			
		  wc.setMapOutputKeyClass(Text.class);
		  wc.setMapOutputValueClass(IntWritable.class);
			  
		  wc.setOutputKeyClass(Text.class);
		  wc.setOutputValueClass(IntWritable.class);
		
		  FileInputFormat.setInputPaths(wc, inputPath);
		  FileOutputFormat.setOutputPath(wc, wcOutputPath);
			
		  RunningJob job = JobClient.runJob(wc);
		  job.waitForCompletion();
		  Counters counters = job.getCounters();
		  
		  int numDocs = (int) counters.getCounter(TrecWordCount.Count.DOCS);
		  
		  Configuration conf = new Configuration();
		  conf.set("numDocs", String.valueOf(numDocs));
          conf.set("maxDocNo", String.valueOf(maxDocNo));
		  long milliSeconds = 1000*60*20;
		  conf.setLong("mapred.task.timeout", milliSeconds);
		  conf.setBoolean("mapred.compress.map.output", true);
		  conf.set("mapred.output.compression.type", "BLOCK");
		  conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		  
		  JobConf mi = new JobConf(conf, TrecMutualInfoPair.class);
		  mi.setNumReduceTasks(numReducers);
		  mi.setJobName("trec-pmi");
	      DistributedCache.addCacheFile(wordList.toUri(), mi);
          
		  mi.setMapperClass(TrecMutualInfoMapper.class);
		  mi.setReducerClass(TrecMutualInfoReducer.class);
		  
		  mi.setInputFormat(TrecDocumentInputFormat.class);
		  mi.setOutputFormat(TextOutputFormat.class);
		
		  mi.setMapOutputKeyClass(Text.class);
		  mi.setMapOutputValueClass(IntWritable.class);
		  
		  mi.setOutputKeyClass(Text.class);
		  mi.setOutputValueClass(DoubleWritable.class);
	
		  FileInputFormat.setInputPaths(mi, inputPath);
		  FileOutputFormat.setOutputPath(mi, miOutputPath);
		  
		  FileInputFormat.setInputPaths(mi, inputPath);
		  FileOutputFormat.setOutputPath(mi, miOutputPath);
		  
		  FileSystem fs = FileSystem.get(conf);
		  Path pathPattern = new Path(wcOutputPath, "part-[0-9]*");
		  FileStatus [] list = fs.globStatus(pathPattern);
		  for (FileStatus status: list) {
			  String name = status.getPath().getName();
			  System.out.println("Adding cache file " + status.getPath().toUri().toString());
			  DistributedCache.addCacheFile(new Path(wcOutputPath, name).toUri(), mi); 
		  }
		
		  RunningJob miJob = JobClient.runJob(mi);
		  miJob.waitForCompletion();
		  
		  return 0;
	  }
	
	  public static void main(String[] args) throws Exception {
		  int res = ToolRunner.run(new Configuration(), new TrecMutualInfoPair(), args);
		  System.exit(res);
	  }
}

