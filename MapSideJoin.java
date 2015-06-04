

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.HashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.filecache.DistributedCache;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class MapSideJoin 
{
		public static class UserMapper extends Mapper<LongWritable, Text, Text, Text>
		{
			
			private HashMap<String, String> userInfoLookup = new HashMap<String, String>();
			
			@Override
			protected void setup(Context context)throws IOException, InterruptedException
			{
				// TODO Auto-generated method stub
				super.setup(context);
	
				@SuppressWarnings("deprecation")
				Path[] files = DistributedCache.getLocalCacheFiles(context.getConfiguration());
				
				for(Path myFile: files)
				{
					String fileName=myFile.getName();
					File file =new File(fileName.trim());
					FileReader fr= new FileReader(file);
					BufferedReader br= new BufferedReader(fr);
					String line;
					while ((line = br.readLine()) != null) {
						String[] splitArray = line.split("::");
						if(splitArray != null && !splitArray[0].isEmpty()) {
							userInfoLookup.put(splitArray[0].trim(),line);
						}
					}
					br.close();
				}
				
			}
	
	
			public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException
			{
				String[] ratingArray = value.toString().split("::");
				
				Configuration conf = context.getConfiguration();
				String inputMovieId = conf.get("movieId").toString().trim();
				if(ratingArray != null && !ratingArray[0].isEmpty())
				{
					String userInfo = userInfoLookup.get(ratingArray[0]);
					String[] userInfoSplit = userInfo.split("\\::");
					if(userInfo != null) 
					{
						if(inputMovieId.equals(ratingArray[1].trim()))
						{
							int rating = Integer.parseInt(ratingArray[2].trim());
							if(rating>=4)
							{
								String userId = userInfoSplit[0];
								String gender = userInfoSplit[1];
								String age = userInfoSplit[2];
								context.write(new Text(userId), new Text(gender + "\t" + age));
							}
								
						}
					}
				}
			}
	
		}



		public static class Reduce extends Reducer<Text,Text,Text,Text> 
		{
			private Text result = new Text();
			private Text myKey = new Text();

			public void reduce(Text key, Iterable<Text> values,Context context ) throws IOException, InterruptedException 
			{
				
				for (Text val : values)
				{
					result.set(val.toString());
					myKey.set(key.toString());
					context.write(myKey,result );
				}
			}		
		}
	
	
	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

		if (otherArgs.length != 4)
		{
			System.err.println("Give all required args");
			System.exit(2);
		}
		
		conf.set("movieId", otherArgs[3]);// movie id as arg
		DistributedCache.addCacheFile(new URI(args[0]), conf);// add file to cache

		Job job = Job.getInstance(conf, "listusersmapside"); 
		job.setJarByClass(Solution2.class);
		job.setReducerClass(Reduce.class);
		job.setMapperClass(UserMapper.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		
		FileInputFormat.addInputPath(job,new Path(otherArgs[1]));
		FileOutputFormat.setOutputPath(job, new Path(otherArgs[2]));

		job.waitForCompletion(true);
	}

}