package cs455;

import java.io.IOException;
import java.util.StringTokenizer;
 
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



public class BigramCreator extends Configured implements Tool {
	
	public static class MapClass extends Mapper<LongWritable, Text, Text, IntWritable>{
		private final static IntWritable one 		= new IntWritable(1);
	    	private Text word 				= new Text();
	    	 
		public void map(LongWritable key, Text value, Context output) throws IOException, InterruptedException {
			String word1 				= null,word2;
	    		String line 				= value.toString();
	    	StringTokenizer itr = new StringTokenizer(line);
	    	if(itr.hasMoreTokens())
	    		word1 = Util.normStr(itr.nextToken());
	      
	    	while (itr.hasMoreTokens()) {
	    		word2 = Util.normStr(itr.nextToken());
	    		if (!word1.isEmpty() && !word2.isEmpty()){
	    			word.set(Util.join(word1, word2));
	    			output.write(word, one);
	    			word.set(Util.join(word1, "0"));
	    			output.write(word, one);

	    		}//if
	    		word1 = word2;
	    	}//while
		}// map method
	}// MapClass
	  
	public static class ReduceClass extends Reducer<Text, IntWritable, Text, IntWritable> {
	 
		public void reduce(Text key,  Iterable<IntWritable> values, Context output) throws IOException, InterruptedException {
			int sum = 0;
			for(IntWritable val: values){
				sum += val.get();
			}
			output.write(key, new IntWritable(sum));	
		}//reduce method
	}// ReduceClass
	 
	static int printUsage() {
		System.out.println("cs455.BigramCreator [-r <reduces>] <input> <output>");
		ToolRunner.printGenericCommandUsage(System.out);
	    return -1;
	}
	 
	public int run(String[] args) throws Exception {	
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());
		job.setJarByClass(BigramCreator.class);
		job.setJobName("bigramCreator");
	 
		// mapper's output
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);
	 
		job.setMapperClass(MapClass.class);        
		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);
		int inputLocation = 0;
		try {
			if ("-r".equals(args[0])){
				if (args.length != 4)
					return printUsage();
	    		job.setNumReduceTasks(Integer.parseInt(args[1]));
	    		inputLocation = 2;
			}
		}catch (NumberFormatException except) {
	    	System.out.println("ERROR: Integer expected instead of " + args[1]);
	    	return printUsage();
	    }
		FileInputFormat.setInputPaths(job, new Path(args[inputLocation]));
		FileOutputFormat.setOutputPath(job, new Path(args[inputLocation+1]));

		boolean success = job.waitForCompletion(true);
	    return success ?0:1;
	}
	 
	 
	public static void main(String[] args) throws Exception {
		if (args.length<2)
			System.exit(printUsage());
		else
			System.exit(ToolRunner.run(new BigramCreator(), args));
	  }
}
