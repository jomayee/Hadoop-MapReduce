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

public class TFIDF extends Configured implements Tool {


   private static final Logger LOG = Logger .getLogger( TFIDF.class);
	private static final String Count="count";
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new TFIDF(), args);
      System .exit(res);
   }
    
   public int run( String[] args) throws  Exception {
	
	//Temp directory is to be created so as to save the output of map-red job one.
	String outputTempDir = "Temp";
	  Job job1  = Job .getInstance(getConf(), " tfidf ");
      job1.setJarByClass( this .getClass());
      FileInputFormat.addInputPaths(job1,  args[0]);
	 //The temp directory is given as path to save the output of reducer one.
      FileOutputFormat.setOutputPath(job1,  new Path(args[1]+outputTempDir));
      job1.setMapperClass( Map1 .class);
      job1.setReducerClass( Reduce1 .class);
      job1.setOutputKeyClass( Text .class);
      job1.setOutputValueClass( IntWritable .class);
	       
	  boolean success = job1.waitForCompletion(true);
	  
	  //Second job starts only when first job is successfully completed.
	if (success) 
	{
	//get the count of the number of files in the input path, so we can use this count later while calculating the tfidf.
	 FileSystem filesystem = FileSystem.get(getConf());
	Path path=new Path(args[0]);
	ContentSummary content= filesystem.getContentSummary(path);
	long filecount = content.getFileCount();
	//getConf is set here because we have to use this count later in reducer2. 
	 getConf().set(Count, ""+filecount);
	
    Job job2 = Job.getInstance(getConf(), " tfidf ");
	job2.setJarByClass(this .getClass());
	
	job2.setMapperClass(Map2.class);
    job2.setReducerClass(Reduce2.class);
	job2.setOutputKeyClass( Text .class);
    job2.setOutputValueClass( Text .class);
	//The file path for input to mapper 2 is the tempdir
    FileInputFormat.addInputPath(job2, new Path(args[1]+outputTempDir));
    FileOutputFormat.setOutputPath(job2, new Path(args[ 1]));
    success = job2.waitForCompletion(true);
	
	return job2.waitForCompletion( true)  ? 0 : 1;
   }
   
   
   return job1.waitForCompletion( true)  ? 0 : 1;
   }
   public static class Map1 extends Mapper<LongWritable ,  Text ,  Text ,  IntWritable > {
      private final static IntWritable one  = new IntWritable( 1);
      private Text word  = new Text();
	  String filename = new String();
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");
	  
	  
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {

		//The filename of the input split is taken
         String line  = lineText.toString();
         FileSplit fsplit = (FileSplit) context.getInputSplit();
		 String filename = fsplit.getPath().getName();
		 
         for ( String word  : WORD_BOUNDARY .split(line)) {
            if (word.isEmpty()) {
               continue;
            }
			//string &#&#& and filename of the file where the input split is taken is appended to the word 
			StringBuilder str=new StringBuilder();
			str.append(word.toString()).append("&#&#&").append(filename);
			String a = str.toString();
			context.write(new Text(a),one);
         }
      }	  
	  
   }

   public static class Reduce1 extends Reducer<Text ,  IntWritable ,  Text ,  DoubleWritable > {
      @Override 
      public void reduce( Text word,  Iterable<IntWritable > counts,  Context context)
         throws IOException,  InterruptedException {
         int sum  = 0;
         for ( IntWritable count  : counts) {
            sum  += count.get();
         }
		 //The term frequency is calculated using the equation and returned as double value
		 double logvalue=1+Math.log10(sum);
         context.write(word,  new DoubleWritable(logvalue));
      }
   }
   
   //The output from the reducer 1 would be the word with filename and the word frequency
   public static class Map2 extends Mapper<LongWritable ,  Text ,  Text ,  Text > {
      private Text word  = new Text();
	  private Text value=new Text(); 
	  
      private static final Pattern WORD_BOUNDARY = Pattern .compile("\\s*\\b\\s*");	  	 
		
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
		//The Text line is converted to String
         String line  = lineText.toString();      		 
         if (line.isEmpty()) {
               return;
            }	
				//The lines are split based on the patter appended in the earlier mapper.
			String[] parts = line.split("&#&#&");
			//The word is saved in currentWord1 and the filename, wordfrequency are split based on tab.
			Text currentWord1  = new Text(parts[0]);	
						
			Text value=new Text(parts[1]);
			String[] parts1 = (value.toString()).split("\t");
			//The values are modified in the format needed, word, filename=wordfrequency
			Text value1=new Text(parts1[0]);
			Text value2=new Text(parts1[1]);
			StringBuilder str=new StringBuilder();
			str.append(value1.toString()).append("=").append(value2.toString());
			String b = str.toString();
			//Text text=new Text();
			//text.set(b);
			
			context.write(currentWord1,new Text(b));

		 }
		}

   
	public static class Reduce2 extends Reducer<Text ,  Text ,  Text ,  DoubleWritable > {
      
	  //The reducer gets the outputs from mapper two in the format, key as word, multiple values in the format filename=wordfrequency
			  public void reduce( Text word,  Iterable<Text> values,  Context context)
				 throws IOException,  InterruptedException {
				 double IDF=0;
				 double tfidf=0;
				 //From job2 configuration, the filecount is obtained.
				 long filecount = context.getConfiguration().getLong(Count, 1);
				 //An array is initialized
				ArrayList<Text> tmp = new ArrayList<Text>();
			
			//For every value obtained, append the value to an array, so the number of files for one word is the size of the array, we can use this value to calculate IDF.
				for (Text val: values)
				{
					tmp.add(new Text(val.toString()));
				}
				double arraysize=tmp.size();
		 
				IDF=(Math.log10(1+(filecount/arraysize)));
		 
				for (int i=0; i<arraysize; i++)
					{
					 String item = tmp.get(i).toString();
					 String[] a=item.split("=");
					 Text value1=new Text(a[0]);
					 Text value2=new Text(a[1]);
					//For every element in the array, i.e. (filename=wordfrequency) pattern values, we split the value by = and later append the string ##### and filename.
					double d=Double.parseDouble(value2.toString());
					StringBuilder str=new StringBuilder();
					str.append(word.toString()).append("#####").append(value1.toString());
					tfidf=Double.parseDouble(value2.toString())*IDF;
					//The tfidf is calcuated
					String b = str.toString();
					//Text text=new Text();
					//text.set(b);
					context.write(new Text(b), new DoubleWritable(tfidf));
				 
   }

}
}
}
