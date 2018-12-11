package org.agonar.bdp2;

import twitter4j.Status;
import twitter4j.TwitterException;
import twitter4j.TwitterObjectFactory;

import java.io.*;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class Preprocess {
	
	static String rawdata="../Data";
	static String preprocessed="../Preprocess/";

	public static void main(String[] args) {
		try {
			File[] files = new File(rawdata).listFiles();
			for (File file : files) {

				FileInputStream fis = null;
				InputStreamReader isr = null;
				BufferedReader br = null;

				fis = new FileInputStream(file);
				isr = new InputStreamReader(fis, "UTF-8");
				br = new BufferedReader(isr);

				System.out.println("File:"+file.getName());
				System.out.println("Path:"+file);

				PrintWriter keywordsWriter = new PrintWriter(preprocessed+file.getName()+"_keywords.csv", "UTF-8");	
				PrintWriter hashtagWriter = new PrintWriter(preprocessed+file.getName()+"_hashtag.csv", "UTF-8");
				PrintWriter userWriter = new PrintWriter(preprocessed+file.getName()+"_user.csv", "UTF-8");				
				PrintWriter statusWriter = new PrintWriter(preprocessed+file.getName()+"_status.csv", "UTF-8");

				String rawJSON;
				while ((rawJSON = br.readLine()) != null) {

					try {
						Status status = TwitterObjectFactory.createStatus(rawJSON);

//						System.out.println("@"+status.getUser().getScreenName()+","+status.getId());
						
						TimeZone tz = TimeZone.getTimeZone("UTC");
						DateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"); 
						df.setTimeZone(tz);
						String tweetDate = df.format(status.getCreatedAt());

//						System.out.println(tweetDate+","+status.getUser().getScreenName()+","+status.getId());
						userWriter.println(tweetDate+","+status.getUser().getScreenName()+","+status.getId());

						String regex = "\\B(\\#[a-zA-Z]+\\b)(?!;)";   
						Pattern p = Pattern.compile(regex);
						Matcher m = p.matcher(status.getText().toLowerCase());

						while(m.find()) {               	   
							hashtagWriter.println(tweetDate+","+m.group()+","+status.getId());
						}

						String regex2 = "http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\\(\\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+|(\\B(\\#[a-zA-Z]+\\b)(?!;)|(?!'.*')\\b[\\w']+\\b)";   				
						Pattern p2 = Pattern.compile(regex2);
						Matcher m2 = p2.matcher(status.getText().toLowerCase());

						while(m2.find()) {               	   
							keywordsWriter.println(tweetDate+","+m2.group()+","+status.getId());
						}

						statusWriter.println(tweetDate+"\t"+status.getText()+"\t"+status.getId());

					} catch (NullPointerException e) {
//						e.printStackTrace();
						//Deletes or non-tweets
					}

				}

				br.close();
				isr.close();
				fis.close();

				keywordsWriter.close();
				hashtagWriter.close();
				userWriter.close();
				statusWriter.close();
			}
			System.exit(0);
		} catch (IOException ioe) {
			ioe.printStackTrace();
			System.out.println("Failed to store tweets: " + ioe.getMessage());
		} catch (TwitterException te) {
			te.printStackTrace();
			System.out.println("Failed to get timeline: " + te.getMessage());
			System.exit(-1);
		} catch (NullPointerException e) {
			e.printStackTrace();
		}
	}

}
