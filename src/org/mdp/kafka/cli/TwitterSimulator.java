package org.mdp.kafka.cli;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.zip.GZIPInputStream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.mdp.kafka.def.KafkaConstants;
import org.mdp.kafka.sim.TwitterStream;

public class TwitterSimulator {
	public static int TWEET_ID = 2;
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
		if(args.length!=3){
			System.err.println("Usage: [tweet_file_gzipped] [tweet_topic] [speed_up (int)]");
			return;
		}
		
		BufferedReader tweets = new BufferedReader(new InputStreamReader(new GZIPInputStream (new FileInputStream(args[0]))));
		
		String tweetTopic = args[1];
		
		int speedUp = Integer.parseInt(args[2]);
		
		Producer<String, String> tweetProducer = new KafkaProducer<String, String>(KafkaConstants.PROPS);
		TwitterStream tweetStream = new TwitterStream(tweets, TWEET_ID, tweetProducer, tweetTopic, speedUp);
		
		Thread tweetThread = new Thread(tweetStream);
		
		tweetThread.start();
		
		try{
			tweetThread.join();
		} catch(InterruptedException e){
			System.err.println("Interrupted!");
		}
		
		tweetProducer.close();
	}
}
