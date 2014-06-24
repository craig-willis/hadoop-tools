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


public class TrecMutualInfoMap extends Configured  implements Tool 
{
    public static enum Count { DOCS  };	

	/**
	 * Mapper implementation:  Tokenize each input string and 
	 * collect all of the unique words.  For each word, collect
	 * an associative array (MapWritable) of all other co-occurring
	 * words.
	 */
	public static class TrecMutualInfoMapper extends MapReduceBase 
  		implements Mapper<LongWritable, TrecDocument, Text, MapWritable> 
	{
        Map<String, Integer> documentFreq = new HashMap<String, Integer>();
	    Set<String> wordList = new HashSet<String>();
	      
        public void configure(JobConf job) 
        {   
            
            try {
                // Read the DocumentWordCount output file
                Path[] files = DistributedCache.getLocalCacheFiles(job);
                for (Path file: files) {
                    if (file.toString().contains("mutual-info-words")) {
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
				OutputCollector<Text, MapWritable> output, Reporter reporter) 
						throws IOException 
		{	    	
		    
		    Analyzer analyzer = new StandardAnalyzer(Version.LUCENE_43);
		    Text term1 = new Text();
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
	    		// Create an associative array containing all 
	    		// co-occurring words.  Note: this is symmetric, 
	    		// but shouldn't effect MI values.
				
			    // Only consider words from the word count phase

				// If wordList provided, only collect terms for those words in the list
				if ((wordList.size() > 0) && (!wordList.contains(word1)))
				   continue;
				
				MapWritable map = new MapWritable();
				term1.set(word1);
				
				for (String word2: words) 
				{	
					if (word1.equals(word2))
						continue;
					
	                if (documentFreq.containsKey(word1) && 
                            documentFreq.containsKey(word2))
                    {
    					
    					Text term2 = new Text();
    
    					term2.set(word2);
    					map.put(term2, one);
                    }
				}
				output.collect(term1, map);
			}
			analyzer.close();
		}
	}

   public static class TrecMutualInfoCombiner extends MapReduceBase 
      implements Reducer<Text, MapWritable, Text, DoubleWritable> 
   {
       public void reduce(Text term, Iterator<MapWritable> values, 
               OutputCollector<Text, DoubleWritable> output, Reporter reporter) 
                       throws IOException 
       {
           // Combine all of the maps from one machine
           while (values.hasNext())
           {
               MapWritable map = values.next();
               Set<Writable> keys = map.keySet();
               for (Writable key: keys)
               {
                   
               }
           }
       }
   }
	
	/**
	 * Reducer implementation: Reads the total number of documents (N) from 
	 * input parameter, reads total document-word counts from output of 
	 * DocumentWordCount MR process.
	 *
	 */
	public static class TrecMutualInfoReducer extends MapReduceBase 
		implements Reducer<Text, MapWritable, Text, DoubleWritable> 
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
                    if (file.toString().contains("mutual-info-words"))
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

		public void reduce(Text term, Iterator<MapWritable> values, 
				OutputCollector<Text, DoubleWritable> output, Reporter reporter) 
						throws IOException 
		{
		
		    Text wordPair = new Text();
	        DoubleWritable mutualInfo = new DoubleWritable();

			// key contains a given word and values contains a set of
			// associative arrays containing all co-occurring words.  Each
			// value represents all co-occurring words in a single document.
			// Collect all of the co-occurrences into a single map.
			Map<String, Integer> jointOccurrences = new HashMap<String, Integer>();
			while (values.hasNext())
			{
				MapWritable map = values.next();
				Set<Writable> keys = map.keySet();
				for (Writable key: keys)
				{
					IntWritable count = (IntWritable)map.get(key);
					String word2 = key.toString();
					
					if (jointOccurrences.containsKey(word2)) {
						int sum = jointOccurrences.get(word2);
						sum += count.get();
						jointOccurrences.put(word2, sum);
					} else {
						jointOccurrences.put(word2, count.get());
					}
				}
			}

			// For each word pair, calculate EMIM.
			String word1 = term.toString();
			for (String word2: jointOccurrences.keySet()) 
			{
				if (documentFreq.containsKey(word1) && 
						documentFreq.containsKey(word2))
				{
					//        | wordY | ~wordY |
					// -------|-------|--------|------
					//  wordX | nX1Y1 | nX1Y0  | nX1
					// ~wordX | nX0Y1 | nX0Y0  | nX0
					// -------|-------|--------|------
					//        |  nY1  |  nY0   | total

					double nX1Y1 = jointOccurrences.get(word2);
					double nX1 = documentFreq.get(word1);
					double nY1 = documentFreq.get(word2);

					double emim = calculateEmim(totalNumDocs, nX1Y1, nX1, nY1);
					
					wordPair.set(word1 + "\t" + word2);
					mutualInfo.set(emim);
					output.collect(wordPair, mutualInfo);
				}
			}	
		}
		
		private double calculateEmim(double N, double nX1Y1, double nX1, double nY1)
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

		  JobConf wc = new JobConf(getConf(), TrecWordCount.class);
		  wc.setJobName("trec-word-count");
			
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
		  long milliSeconds = 1000*60*20;
		  conf.setLong("mapred.task.timeout", milliSeconds);
		  conf.setBoolean("mapred.compress.map.output", true);
		  conf.set("mapred.output.compression.type", "BLOCK");
		  conf.set("mapred.map.output.compression.codec", "org.apache.hadoop.io.compress.GzipCodec");
		  
		  JobConf mi = new JobConf(conf, TrecMutualInfoMap.class);
		  mi.setJobName("mutual-info");
          if (args.length == 4) { 
              Path wordListPath = new Path(args[3]);
              DistributedCache.addCacheFile(wordListPath.toUri(), mi);
          }
		
		  mi.setMapperClass(TrecMutualInfoMapper.class);
		  mi.setReducerClass(TrecMutualInfoReducer.class);
		
		  mi.setInputFormat(TrecDocumentInputFormat.class);
		  mi.setOutputFormat(TextOutputFormat.class);
		
		  mi.setMapOutputKeyClass(Text.class);
		  mi.setMapOutputValueClass(MapWritable.class);
		  
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
		  int res = ToolRunner.run(new Configuration(), new TrecMutualInfoMap(), args);
		  System.exit(res);
	  }
}

