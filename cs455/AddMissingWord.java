package cs455;

import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class AddMissingWord  extends Configured implements Tool {

	public static class MapClass extends Mapper<LongWritable, Text, Text, Text>{
		private String[] sentenceParts;
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			super.setup(context);
			String sentence 		= context.getConfiguration().get("SENTENCE");
			sentenceParts 			= Util.parseText(sentence.toLowerCase(), ' ');
		}
		@Override
		public void  map(LongWritable key, Text line, Context output) throws IOException, InterruptedException{
			String[] parts 		= Util.parseText(line.toString(),'\t');
			String[] words 		= Util.parseText(parts[0].toString(),'|');
			int firstWordIndex 	= Util.contains(sentenceParts, words[0]);
		   	if (firstWordIndex != -1){
		   		if (firstWordIndex < sentenceParts.length -1 && words[1].compareTo(sentenceParts[firstWordIndex + 1])==0)
		   			output.write(new Text(words[0]), new Text(Util.join("0"+"|"+words[1], parts[1])));// "0: key and value are parts of sentence
		   		else
		   			output.write(new Text(words[1]), new Text(Util.join("1"+"|"+words[0], parts[1])));// "1": key after value(value part of sentence)
		   		return;
		   	}
		   	if (Util.contains(sentenceParts, words[1])!=-1)
				output.write(new Text(words[0]), new Text(Util.join("2"+"|"+words[1], parts[1])));// "2": key before value(value part of sentence)
		}
	}//MapClass

	public static class CombineClass extends Reducer<Text, Text, Text, Text> {
		private String[] sentenceParts;
		  
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			super.setup(context);
			String sentence 		= context.getConfiguration().get("SENTENCE");
			sentenceParts 			= Util.parseText(sentence.toLowerCase(), ' ');
		}
		
		@Override
		public void reduce(Text key,  Iterable<Text> values, Context output) throws IOException, InterruptedException {	
			double[] sentenceBeforePro 	= new double[sentenceParts.length];
			double[] sentenceAfterPro 	= new double[sentenceParts.length];
			
			for(Text val: values){
				String[] parts = Util.parseText(val.toString(),'|');
				double probalitiy = Double.parseDouble(parts[2]);
				int firstWordIndex = Util.contains(sentenceParts, parts[1]) ;
				if (parts[1].compareTo(key.toString())==0) continue;
				if (parts[0].compareTo("0")==0)
					output.write(new Text("1"),new Text(key.toString() + "|"+parts[1]+"|"+parts[2] +"|"+ "0" ));
				else
					if (parts[0].compareTo("1")==0)
						sentenceAfterPro[firstWordIndex] += probalitiy;
					else
						sentenceBeforePro[firstWordIndex] += probalitiy;
			}//fort
			
			for (int i = 0; i < sentenceParts.length-1; ++i){
				if (sentenceAfterPro[i] == sentenceBeforePro[i+1] && sentenceAfterPro[i]==0) continue;
				output.write(new Text("1"), new Text(sentenceParts[i]+"|"+key.toString()+"|"+ sentenceAfterPro[i] +"|"+ sentenceBeforePro[i+1]));
			}
		}//reduce method
	}//CombineClass

	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		private String[] sentenceParts;
		  
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			super.setup(context);
			String sentence = context.getConfiguration().get("SENTENCE");
			sentenceParts = Util.parseText(sentence.toLowerCase(), ' ');
		}
		
		@Override
		public void reduce(Text key,  Iterable<Text> values, Context output) throws IOException, InterruptedException {	
			double[] sentenceAfterPro 	= new double[sentenceParts.length];
			double[] sentencePro	 	= new double[sentenceParts.length];
			String[] missingWord		= new String[sentenceParts.length];
			for(Text val: values){
				String[] parts = Util.parseText(val.toString(),'|');
				double probalitiyAfterFirstWord = Double.parseDouble(parts[2]);
				double probalitiyBeforeSecondWord = Double.parseDouble(parts[3]);
				int firstWordIndex = Util.contains(sentenceParts, parts[0]) ;
				
				if (parts[1].compareTo(sentenceParts[firstWordIndex+1])==0)
					sentencePro[firstWordIndex] = probalitiyAfterFirstWord + ((firstWordIndex>0)?sentencePro[firstWordIndex-1]:0);
				else{
					double averagePro =  Math.min(probalitiyAfterFirstWord,probalitiyBeforeSecondWord);
					if (missingWord[firstWordIndex]==null){
						sentenceAfterPro[firstWordIndex] =averagePro;
						missingWord[firstWordIndex] = parts[1];
					}else{
						if (sentenceAfterPro[firstWordIndex]< averagePro){
							missingWord[firstWordIndex] = parts[1];
							sentenceAfterPro[firstWordIndex]= averagePro;
						}
					}
				}
			}//for
			
			int    weakestPartIdx = -1;
			double weakestPart = 2;
			for (int i = 0; i < sentenceParts.length-1; ++i){
				if (weakestPart > sentencePro[i]){
					weakestPart = sentencePro[i];
					weakestPartIdx = i;
				}
				
			}
			String theSentence ="";
			for (int i=0;i<sentenceParts.length;++i){
				theSentence += sentenceParts[i] +" ";
				if (i == weakestPartIdx)
					theSentence += missingWord[weakestPartIdx] + " ";
			}
				
			output.write(new Text("The correct sentence:"), new Text(theSentence));
		}//reduce method
	}//CombineClass

	@Override
	public int run(String[] args) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());
		job.setJarByClass(AddMissingWord.class);
		job.setJobName("addMissinWord");
			
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MapClass.class);
		job.setCombinerClass(CombineClass.class);
		job.setReducerClass(ReducerClass.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		System.out.print("Enter your sentence: ");
		job.getConfiguration().set("SENTENCE",Util.readSentence());
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success = job.waitForCompletion(true);
		return success ?0:1;
	}

	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.out.println(args.length+ "Usage: " + AddMissingWord.class.getName() + 
					" <job_name> <hdfs_input_file> <hdfs_output_dir>");
		}else{
			int ret = ToolRunner.run(new AddMissingWord(), args);
			System.exit(ret);
		}
	}
}
