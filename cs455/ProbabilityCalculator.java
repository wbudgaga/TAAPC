package cs455;



import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;


public class ProbabilityCalculator  extends Configured implements Tool {
	
	public static class TextPair implements WritableComparable<TextPair> {
		private Text first;
		private Text second;
		public TextPair() {
			set(new Text(), new Text());
		}
		public TextPair(String first, String second) {
			set(new Text(first), new Text(second));
		}
		public TextPair(Text first, Text second) {
			set(first, second);
		}
		public void set(Text first, Text second) {
			this.first 			= first;
			this.second 			= second;
		}
		public Text getFirst() {
			return first;
		}
		public Text getSecond() {
			return second;
		}
		@Override
		public void write(DataOutput out) throws IOException {
			first.write(out);
			second.write(out);
		}
		@Override
		public void readFields(DataInput in) throws IOException {
			first.readFields(in);
			second.readFields(in);
		}
		@Override
		public int hashCode() {
			return first.hashCode() * 163 + second.hashCode();
		}
		@Override
		public boolean equals(Object o) {
			if (o instanceof TextPair) {
				TextPair tp 		= (TextPair) o;
				return first.equals(tp.first) && second.equals(tp.second);
			}
			return false;
		}
		@Override
		public String toString() {
			return first + "|" + second;
		}
		@Override
		public int compareTo(TextPair tp) {
			int cmp 			= first.compareTo(tp.first);
			if (cmp != 0) {
				return cmp;
			}
			return second.compareTo(tp.second);
		}
	}	
	public static class TextPairComparator extends WritableComparator {
		private static final Text.Comparator TEXT_COMPARATOR = new Text.Comparator();
		protected TextPairComparator() {
			 super(TextPair.class);
		}

		@Override
		public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
			try {
				int firstL1 		= WritableUtils.decodeVIntSize(b1[s1]) + readVInt(b1, s1);
				int firstL2 		= WritableUtils.decodeVIntSize(b2[s2]) + readVInt(b2, s2);
				
				int cmp 		= TEXT_COMPARATOR.compare(b1, s1, firstL1, b2, s2, firstL2);
				if (cmp != 0) {
					return cmp;
				}
				return TEXT_COMPARATOR.compare(b1, s1 + firstL1, l1 - firstL1, b2, s2 + firstL2, l2 - firstL2);
			} catch (IOException e) {
				throw new IllegalArgumentException(e);
			}
		}
	}
	static {
		WritableComparator.define(TextPair.class, new TextPairComparator());
	}
	
	public static class MapClass extends Mapper<LongWritable, Text, TextPair, Text>{
		@Override
		public void  map(LongWritable key, Text line, Context output) throws IOException, InterruptedException{
			String[] parts 			= Util.parseText(line.toString(),'\t');
			String[] words 			= Util.parseText(parts[0].toString(),'|');
			TextPair textPair 		= new TextPair(words[0],words[1]);
			output.write(textPair, new Text(parts[1]));
		}	
	}//MapClass

	public static class ReduceClass extends Reducer<TextPair, Text, Text, DoubleWritable> {
		private double total			= -1;
		@Override
		public void reduce(TextPair key,  Iterable<Text> values, Context output) throws IOException, InterruptedException {
			if (key.getSecond().toString().compareTo("0") == 0){
				for(Text val: values)
					total 		= Double.parseDouble(val.toString());
				return ;
			}
			for(Text val: values){
				output.write(new Text(key.toString()), new DoubleWritable(Double.parseDouble(val.toString()) / total));
			}					
		}
	}//ReduceClass

	
	@Override
	public int run(String[] args) throws Exception {
		@SuppressWarnings("deprecation")
		Job job 				= new Job(getConf());
		job.setJarByClass(ProbabilityCalculator.class);
		job.setJobName("ProbabilityCalculator");
		job.setSortComparatorClass(TextPairComparator.class);
		job.setOutputKeyClass(TextPair.class);
		job.setOutputValueClass(Text.class);

		job.setMapperClass(MapClass.class);
//		job.setCombinerClass(ReduceClass.class);
		job.setReducerClass(ReduceClass.class);
		
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		//job.setNumReduceTasks(40);
		
		FileInputFormat.setInputPaths(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		
		boolean success 			= job.waitForCompletion(true);
		return success ?0:1;
	}

	public static void main(String[] args) throws Exception{
		if(args.length != 2){
			System.out.println(args.length+ "Usage: " + ProbabilityCalculator.class.getName() + 
					" <job_name> <hdfs_input_file> <hdfs_output_dir>");
		}else{
			int ret 			= ToolRunner.run(new ProbabilityCalculator(), args);
			System.exit(ret);
		}
	}
}

