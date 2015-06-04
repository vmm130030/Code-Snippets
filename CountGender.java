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

public class CountGender {
	
	public static class Map extends Mapper<LongWritable, Text, Text, Text> {
		
		Text agegroup = new Text();
		Text gender=new Text();
	
		@Override
		public void map(LongWritable key, Text value,Context context) throws IOException, InterruptedException {
			String line = value.toString();
		    String tokens[]=line.split("\\::");
		    
		    int age=Integer.parseInt(tokens[2].trim());
		    
		    if(age>0 && age<18){
		    	 agegroup.set(7+"");
		    }else if(age>=18 && age<=24){
		    	agegroup.set(24+"");
		    }else if(age>=25 && age<=34){
		    	agegroup.set(31+"");
		    }else if(age>=35 && age<=44){
		    	agegroup.set(41+"");
		    }else if(age>=45 && age<=55){
		    	agegroup.set(51+"");
		    }else if(age>=56 && age<=61){
		    	agegroup.set(56+"");
		    }else if(age>=62){
		    	agegroup.set(62+"");
		    }
		   
		    gender.set(tokens[1].trim());
		    
		    context.write(agegroup,gender);
		    
		}
	}

	public static class Reduce extends Reducer<Text,Text,Text,Text>{
		
		Text genderandcount=new Text();
	
		@Override
	    public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {

			int maleCount=0,femaleCount=0;
			Iterator<Text> i=values.iterator();
		    while(i.hasNext()){
				String gender=i.next().toString();
				if(gender.equals("M")){
					maleCount++;
				}else if(gender.equals("F")){
					femaleCount++;
				}
			}

			genderandcount.set("M	"+maleCount);
		    context.write(key,genderandcount);
		    
		    genderandcount.set("F	"+femaleCount);
		    context.write(key,genderandcount);
	    }
	}

	public static void main(String[] args) throws Exception {
	    Configuration conf = new Configuration();
	    Job job = Job.getInstance(conf, "problem2");
	    job.setJarByClass(Problem2.class);
	    
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
