package org.bigdata.TwitterDataAnalysis;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;
import org.machine.BayesClassification;
import org.machine.Filter;
import org.machine.TestFile;
import org.machine.Utility;

import twitter4j.Status;
import twitter4j.StatusUpdate;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;

public class Twitter {
	
	public static String filterStopWords = "y"; 
	static ConfigurationBuilder cb = new ConfigurationBuilder();
	static {
		Logger.getLogger("org.apache.spark").setLevel(Level.WARN);
		Logger.getLogger("org.apache.spark.storage.BlockManager").setLevel(
				Level.ERROR);
		cb.setUseSSL(true);
	}
	static twitter4j.Twitter twitter = new TwitterFactory(cb.build()).getInstance();
	static FileWriter fw;
	static BufferedWriter bw;
	static FileWriter fw2;
	static BufferedWriter bw2;
	static ArrayList<String> userIds = new ArrayList<String>();

	@SuppressWarnings({ "serial" })
	public static void main(String[] args) throws Exception {

		// Configuring Twitter credentials
		String apiKey = "gUYDmWwNRfWBe8dzG99Arm5eB";
		String apiSecret = "cQOdgP5BC7qgMnrh0dBmBaWGNP6Zmh1qH5j4rlPCU77zxk3YLc";
		String accessToken = "261659830-L02fg6WP5ye76cErMjhcTR3qWgrZBKLnU9YsoO2z";
		String accessTokenSecret = "1S4NccMQehdp0A9WfqzXQtTkRVq4PK8TcKF7w4EMK1Ua3";
		
		createFile("Barca.txt");
		createFile("RM.txt");

		setupMachine();
		
		// global filters
		String[] filter = new String[] {"BigDataDemo15"};

		twitter.setOAuthConsumer(apiKey, apiSecret);
		twitter.setOAuthAccessToken(new AccessToken(accessToken,
				accessTokenSecret));

		JavaStreamingContext ssc = new JavaStreamingContext(new SparkConf(),
				new Duration(1000));
		JavaDStream<Status> tweets = TwitterUtils.createStream(ssc,
				twitter.getAuthorization(), filter);

		
		// get filtered english twits 
		JavaDStream<Status> statuses = tweets
				.filter(new Function<Status, Boolean>() {

					private static final long serialVersionUID = 1L;
					
					public synchronized Boolean call(Status status) {
						
						if (status.getLang().equals("en")) {
							return true;
						} else {
							return false;
						}
					}
				});
		

		
		
		getRepliedUsers();
		
		JavaDStream<Status> replies = statuses
				.filter(new Function<Status, Boolean>() {
					public synchronized Boolean call(Status status)
							throws IOException {
						
						
						String statusString=removeUrl(status.getText().trim());
						String statusStringWithHandle=status.getUser().getScreenName()+"::"+statusString;
						
						if (statusString.contains("#Messi")
								|| statusString.contains("#Barcelona")) 
						{
							
							if(classify(statusString)==1 )
							{
								System.out.println(statusStringWithHandle+" => Postive Tweet");
								fileWrite(bw, statusStringWithHandle+" => Postive Tweet");
								if (userIds.contains(status.getUser().getScreenName().trim())) {
									sendReply(status," I think you might find this interesting www.googledrive.com/host/0B13H1t_oOCq5WVN3ckFXZW1sQ0U");
								}
							
							}
							else
							{
								System.out.println(statusStringWithHandle+" => Negative Tweet");
								fileWrite(bw, statusStringWithHandle+" => Negative Tweet");
								if (userIds.contains(status.getUser().getScreenName().trim())) {
									sendReply(status," I think you might find this interesting www.googledrive.com/host/0B13H1t_oOCq5OHJyc3pDMWNFXzA");
								}
							}
							

						} else if(statusString.contains("#Ronaldo")
								|| statusString.contains("#RealMadrid"))
						{
							
							if(classify(statusString)==0 )
							{
								System.out.println(statusStringWithHandle+" => Negative Tweet");
								fileWrite(bw2, statusStringWithHandle+" => Negative Tweet");
								if (userIds.contains(status.getUser().getScreenName().trim())) {
									sendReply(status," I think you might find this interesting www.googledrive.com/host/0B13H1t_oOCq5UE5vZ1VUdjVZdjQ");
								}
							}
							else
							{
								System.out.println(statusStringWithHandle+" => Postive Tweet");
								fileWrite(bw2, statusStringWithHandle+" => Postive Tweet");
								if (userIds.contains(status.getUser().getScreenName().trim())) {
									sendReply(status," I think you might find this interesting www.googledrive.com/host/0B13H1t_oOCq5bjlYY2RocWU3TXM");
								}
							}
							
						}
					
						return true;
						
					}// end call

				}// end func
				);
		
		
		// status object into string format (handle::statusText)
				JavaDStream<String> statusesString = replies
						.map(new Function<Status, String>() {

							private static final long serialVersionUID = 1L;
							
							public synchronized String call(Status status) {
							
								return "";
							}
						});
	
	    statusesString.print();
	    ssc.checkpoint("E:\\UTDSpring2015\\Big_Data\\Final_Project\\checkpoint");			    	   
		ssc.start();
		ssc.awaitTermination();

	}
	
	 synchronized static void setupMachine()
	{
		String noIntent_Learning_dir = ".\\trainNoIntent";
		String intent_Learning_dir = ".\\trainIntent";
		String stop_file = ".\\stop_words.txt";
		// reading the spam files
		File noIntentDir = new File(noIntent_Learning_dir);
		File[] noIntentFiles = noIntentDir.listFiles();

		// reading the Ham files
		File hamDir = new File(intent_Learning_dir);
		File[] hamFiles = hamDir.listFiles();

		/// setting total docs in the learning set
		int totalDocs = noIntentFiles.length + hamFiles.length;
		 
		// creat stop words filter
		File stopFile = new File(stop_file );
        Filter filter = new Filter();
	    filter.createListOfStopWords(stopFile);
		Utility obj = new Utility(noIntentFiles,hamFiles);
        obj.setTOTALDOCS(totalDocs);
        
        // this will build the vocabulary 
        obj.createVocabulary();
        
        // create bayes classifier
        BayesClassification bayes = new BayesClassification();
        int laplcae  = 3;
        bayes.laplaceSmoothing = laplcae;
        bayes.IntializeConditionalProbForSpamClass();
        bayes.IntializeConditionalProbForHamClass();
    
        System.out.println("\n\n\tBuilding model  Using Naive Bayes...Please Wait..");

	}
	synchronized static void getRepliedUsers() throws IOException{
		
		FileReader fr = new FileReader("users.txt"); 
		BufferedReader br = new BufferedReader(fr); 
		
		String s; 
		while((s = br.readLine()) != null) { 
			userIds.add(s.trim());
		} 
		fr.close(); 
	}
	/**
	 * this method checks accuracy on the spam messages
	 * @param testSpamFiles
	 * @return accuracy
	 */
	 synchronized static int classify(String tweet)
	{
		TestFile testObj = new TestFile(tweet);
        testObj.ExtractTokens();
        double probOfBeingNegative = testObj.calculateBayesForSpam();
        double probOfBeingPositive = testObj.calculateBayesForHam();
  
        if(probOfBeingNegative > probOfBeingPositive)
        {
        	
        	return 0;
        	
        }else
        {
        	return 1;
        }
            
	}
	synchronized static void fileWrite(BufferedWriter bw, String status)
			throws IOException {

		bw.write(status + "\n");
		bw.flush();

	}

	synchronized static void createFile(String fileName) throws IOException {

		final File file = new File(".\\" + fileName);

		// if file does not exist then create it
		if (!file.exists()) {
			file.createNewFile();
		}

		if (fileName.equals("Barca.txt")) {
			fw = new FileWriter(file.getAbsoluteFile(), true);
			bw = new BufferedWriter(fw);
			
		} else if (fileName.equals("RM.txt")) {
			fw2 = new FileWriter(file.getAbsoluteFile(), true);
			bw2 = new BufferedWriter(fw2);
			
		}

	}

	
	synchronized static void sendReply(Status status,String message) {

		StatusUpdate st = new StatusUpdate("@"+status.getUser().getScreenName()+message);
		st.inReplyToStatusId(status.getId());

		try {
			twitter.updateStatus(st);
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	synchronized static String removeUrl(String commentstr) {
		String urlPattern = "((https?|ftp|gopher|telnet|file|Unsure|http):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
		Pattern p = Pattern.compile(urlPattern, Pattern.CASE_INSENSITIVE);
		Matcher m = p.matcher(commentstr);
		int i = 0;
		while (m.find()) {
			commentstr = commentstr.replaceAll(m.group(i), "").trim();
			i++;
		}
		return commentstr;
	}

	

}