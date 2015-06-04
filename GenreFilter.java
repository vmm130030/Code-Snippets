import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;



public class GenreFilter {
	
public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		Text movietitleValue = new Text();
		Text genreKey=new Text();
		String inputGenre;
		
		@Override
		public void setup(Context context) {
			Configuration conf = context.getConfiguration();
			String input = conf.get("inputGenre");
			inputGenre = input;
		}
	
		
		@Override
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			String line = value.toString();
		    String tokens[]=line.split("\\::");
		    String genres[]=tokens[2].split("\\|"); 
		    
		    for(String g:genres){
		    	String genre=g.trim();
		    	if(genre.equals(inputGenre))
		    	{
		    		genreKey.set(genre);
		    		movietitleValue.set(tokens[1].trim());
		    		context.write(genreKey,movietitleValue);
		    	}
		    }
		    
		}
	}

	public static class Reduce extends Reducer<Text, Text, Text, Text> {
		Text movietitle=new Text();
		
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
				
			   Iterator<Text> i=values.iterator();
			    while(i.hasNext()){
			    	movietitle.set(i.next());
			    	context.write(key,movietitle);
				}
		}
	}

	public static void main(String[] args) throws Exception {
		    Configuration conf = new Configuration();
		    conf.set("inputGenre", args[2]);
		    
		    Job job = Job.getInstance(conf, "problem3");
		    job.setJarByClass(Problem3.class);
		    
		    job.setMapperClass(Map.class);
		    //job.setCombinerClass(Reduce.class);
		    job.setReducerClass(Reduce.class);
		    
		    job.setOutputKeyClass(Text.class);
		    job.setOutputValueClass(Text.class);
		    
		    FileInputFormat.addInputPath(job, new Path(args[0]));
		    FileOutputFormat.setOutputPath(job, new Path(args[1]));
		    
		    System.exit(job.waitForCompletion(true) ? 0 : 1);	
	}


}
