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
import java.math.*;

public class Ranker extends Configured implements Tool {

   private static final Logger LOG = Logger .getLogger( Ranker.class);
	
   public static void main( String[] args) throws  Exception {
      int res  = ToolRunner .run( new Ranker(), args);
      System .exit(res);
   }

   public int run( String[] args) throws  Exception {
      Job job  = Job .getInstance(getConf(), " ranker ");
      job.setJarByClass( this .getClass());
		
      FileInputFormat.addInputPaths(job,  args[0]);
      FileOutputFormat.setOutputPath(job,  new Path(args[ 1]));
      job.setMapperClass( Map .class);
      job.setReducerClass( Reduce .class);
      job.setOutputKeyClass( Text .class);
      job.setOutputValueClass( Text .class);
	  job.setNumReduceTasks(1);
      return job.waitForCompletion( true)  ? 0 : 1;
   }
   
   public static class Map extends Mapper<LongWritable ,  Text ,  DoubleWritable ,  Text > {
            	  
      public void map( LongWritable offset,  Text lineText,  Context context)
        throws  IOException,  InterruptedException {
		
         String line  = lineText.toString();
		 if (line.isEmpty()) {
               return;
            }	
		
        String[] parts=line.split("\t");
		double result = Double.parseDouble(parts[1].toString()));
		
		result*=-1;
		 		          
			context.write(new DoubleWritable(result), new Text(parts[0]));
         }
      }

   public static class Reduce extends Reducer<Text ,  Text ,  Text ,  Text > {
      @Override 
      public void reduce( Text word,  Iterable<Text> counts,  Context context)
         throws IOException,  InterruptedException {
         
         for ( Text count  : counts) {
            //sum  += count.get();
         //}
		 
         context.write((word*(-1)),  count);
      }
   }
}
}
