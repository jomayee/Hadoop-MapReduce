//Jyothirmayi Panda (800932963), jpanda@uncc.edu

package org.myorg;

import java.lang.*;
import java.io.IOException;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.Logger;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.math.*;
import org.apache.hadoop.io.ArrayWritable;
import java.util.ArrayList;
import org.apache.hadoop.mapred.JobConf;
import java.util.List;
import java.io.File;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import java.util.Set;
import java.util.Arrays;

public class Search extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( Search.class);
   //A global string value is declared
	private static final String value = "values";
	//A String arraylist is created, we do this to append the args which contain space seperated user queries.
	static ArrayList<String> database = new ArrayList<String>();
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Search(), args);
      System .exit(res);
   }
  public int run( String[] args) throws  Exception {	   
	 //The first argument would be input file path, second is outputfile path, so we begin from third one and append till the array size.
	   int j=2;
		for(int i=2; i<args.length; i++)
		{
			String argument=args[i];
			database.add(argument);
		}	
	
	  Job job1  = Job .getInstance(getConf(), " search ");
      job1.setJarByClass( this .getClass());
      FileInputFormat.addInputPaths(job1,  args[0]);
      FileOutputFormat.setOutputPath(job1, new Path(args[1]));
	  String[] input=new String[database.size()];
	  
	  input=database.toArray(input);
      job1.setMapperClass( Map1 .class);
      job1.setReducerClass( Reduce1 .class);
      job1.setOutputKeyClass( Text .class);
      job1.setOutputValueClass( Text .class);
	  //job configuration is set, so that the arguments given can be called later from the mapper
	  job1.getConfiguration().setStrings(value, input);
	   boolean success = job1.waitForCompletion(true);
	  	  
   return job1.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map1 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {       
	  double tfidf;      
	 public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {  

		
         String line  = lineText.toString();      		 
         if (line.isEmpty()) {
               return;
            }			
			String[] parts = line.split("#####");
		
		//The arguments array is called
			String[] input=context.getConfiguration().getStrings(value);
		
		//For every user query entered, the word is verified if it equals (ignoring case) the key.
		//If yes, then the key is split by tab, for fetching the TFIDF score of that word.
			for(String key: input)
			
			{
				if(key.equalsIgnoreCase(parts[0].toString()))
				{				
											
				String[] parts1 = parts[1].split("\t");				
				context.write(new Text(parts1[0]),new Text(parts1[1]));
         }
		 }
      }	  
   }
   public static class Reduce1 extends Reducer<Text ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( Text word,  Iterable<Text > counts,  Context context)
         throws IOException,  InterruptedException {
         double sum  = 0;
		 //The TFIDF scores along with filenames are given as input to the reducer.
		 //For every value in the TFIDF scores, the summation is done. If only one value exists, the value is returned. 
 		 for (Text value : counts)
		 {
			 try{
			 sum  += Double.parseDouble(value.toString());
		 }
		 catch (Exception e) {
                    context.write(word, value);
                }
            }
            Text result = new Text("" + sum);
            context.write(word, result);
		 }
			}
         
		 
   
}
