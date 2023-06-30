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
import org.mdp.kafka.sim.YouTubeStream;

public class YouTubeSimulator {
	public static int video_ID = 0;
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
		if(args.length!=3){
			System.err.println("Usage: [video_file] [video_topic] [speed_up (int)]");
			return;
		}
		
		BufferedReader videos = new BufferedReader(new InputStreamReader(new FileInputStream(args[0])));
		
		String videoTopic = args[1];
		
		int speedUp = Integer.parseInt(args[2]);
		
		Producer<String, String> videoProducer = new KafkaProducer<String, String>(KafkaConstants.PROPS);
		YouTubeStream videoStream = new YouTubeStream(videos, video_ID, videoProducer, videoTopic, speedUp);
		
		Thread videoThread = new Thread(videoStream);
		
		videoThread.start();
		
		try{
			videoThread.join();
		} catch(InterruptedException e){
			System.err.println("Interrupted!");
		}
		
		videoProducer.close();
	}
}
