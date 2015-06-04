import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MaleFilter {

	public static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {
	
		private Text userid = new Text();
	
		@Override
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {

			String line = value.toString();
		    String tokens[]=line.split("\\::");
		    if(tokens[1].trim().equals("M") && Integer.parseInt(tokens[2].trim())<=7){
		    	userid.set(tokens[0].trim());
		    	context.write(userid,NullWritable.get());
		    }
		    
		}
	}

	public static class Reduce extends Reducer<Text, NullWritable, Text, NullWritable> {
		
		@Override
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			context.write(key,NullWritable.get());
		}
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "problem1");
	    job.setJarByClass(Problem1.class);
	    
	    job.setMapperClass(Map.class);
	    job.setReducerClass(Reduce.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    
	    FileInputFormat.addInputPath(job, new Path(args[0]));
	    FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);	
   }
}