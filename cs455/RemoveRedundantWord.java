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


public class RemoveRedundantWord  extends Configured implements Tool {

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
			String[] parts = Util.parseText(line.toString(),'\t');
			String[] words = Util.parseText(parts[0].toString(),'|');

			int firstWordIndex = Util.contains(sentenceParts,words[0]);
		   	if (firstWordIndex == -1 || firstWordIndex >= sentenceParts.length -1)
				return;
		   	if(sentenceParts[firstWordIndex+1].compareToIgnoreCase(words[1])==0 || 
		   			(firstWordIndex < sentenceParts.length -2 && sentenceParts[firstWordIndex + 2].compareToIgnoreCase(words[1])==0))
		   		output.write(new Text("0"), new Text(Util.join(words[0]+"|"+words[1], parts[1])));
		}
	}//MapClass

	
	public static class ReducerClass extends Reducer<Text, Text, Text, Text> {
		private String[] sentenceParts;
		private double[] sentencePartsPro;
		private double[]   sentencePartsSkipOnePro;
		  
		@Override
		protected void setup(Context context) throws IOException,InterruptedException {
			super.setup(context);
			String sentence = context.getConfiguration().get("SENTENCE");
			sentenceParts = Util.parseText(sentence.toLowerCase(), ' ');
			sentencePartsPro 	= new double[sentenceParts.length];
			sentencePartsSkipOnePro = new double[sentenceParts.length];
		}
						
	    public void reduce(Text key,  Iterable<Text> values, Context output) throws IOException, InterruptedException {
		   	
			for(Text val: values){
				String[] parts = Util.parseText(val.toString(),'|');
				int firstWordIndex = Util.contains(sentenceParts,parts[0]);
				double probalitiy = Double.parseDouble(parts[2]);
				if (sentenceParts[firstWordIndex+1].compareTo(parts[1])==0)
					sentencePartsPro[firstWordIndex] = probalitiy;
				else 
					sentencePartsSkipOnePro[firstWordIndex+1]= probalitiy;
			}

		/*	sentencePartsSkipOnePro[0] = 0.0001;
			sentencePartsSkipOnePro[sentencePartsSkipOnePro.length - 1] = 0.0001; 
*/			double max  = 2.0;
			int    minIndex = -1;
			for (int i=0;i<sentenceParts.length-1;++i){
			//	output.write(new Text(i+""), new Text(sentencePartsPro[i]+"|"+sentencePartsSkipOnePro[i]));
				double partPro = Math.min(sentencePartsPro[i] , ((i>0)?sentencePartsPro[i-1]:1));
//				partPro = sentencePartsSkipOnePro[i] + partPro;
				if (partPro < max){
					max = partPro;
					minIndex = i;
				}
			}
			String theSentence ="";
			for (int i=0;i<sentenceParts.length;++i){
				if (i == minIndex)
					continue;
				theSentence += sentenceParts[i] +" ";
			}
			output.write(new Text("The correct sentence:"), new Text(theSentence));
	    }//reduce method
	}//CombineClass

	@Override
	public int run(String[] args) throws Exception {
		@SuppressWarnings("deprecation")
		Job job = new Job(getConf());
		job.setJarByClass(RemoveRedundantWord.class);
		job.setJobName("removeRedundantWord");
		
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		job.setMapperClass(MapClass.class);
	//	job.setCombinerClass(ReducerClass.class);
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
			System.out.println(args.length+ "Usage: " + RemoveRedundantWord.class.getName() + 
					" <job_name> <hdfs_input_file> <hdfs_output_dir>");
		}else{
			int ret = ToolRunner.run(new RemoveRedundantWord(), args);
			System.exit(ret);
		}
	}

}
