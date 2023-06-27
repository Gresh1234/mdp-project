package org.mdp.kafka.cli;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.LinkedList;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mdp.kafka.def.KafkaConstants;

public class BurstDetector {
	public static final int FIFO_SIZE = 50;
	public static final int EVENT_START_TIME_INTERVAL = 50 * 1000;
	public static final int EVENT_END_TIME_INTERVAL = 2 * EVENT_START_TIME_INTERVAL;
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
		Properties props = KafkaConstants.PROPS;

		// randomise consumer ID so messages cannot be read by another consumer
		//   (or at least it's more likely that a meteor wipes out life on Earth)
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		LinkedList<ConsumerRecord<String, String>> fifo = new LinkedList<ConsumerRecord<String, String>>();
		
		boolean inEvent = false;
		int events = 0;
		
		consumer.subscribe(Arrays.asList(args[0]));
		
		try{
			while (true) {
				// every ten milliseconds get all records in a batch
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
				
				// for all records in the batch
				for (ConsumerRecord<String, String> record : records) {
					fifo.add(record);
					
					if(fifo.size()>=FIFO_SIZE) {
						ConsumerRecord<String, String> oldest = fifo.removeFirst();
						long gap = record.timestamp() - oldest.timestamp();
						
						if(gap <= EVENT_START_TIME_INTERVAL && !inEvent) {
							inEvent = true;
							events++;

							Date date = new Date(oldest.timestamp());
							String[] tabs = oldest.value().split("\t");
							
							System.out.println("START event-id:"+ events +": start:"+date);
							System.out.println("video_id: "+tabs[0]);
							System.out.println("title: "+tabs[1]);
							System.out.println("channel_title: "+tabs[3]);
							System.out.println("rate: "+FIFO_SIZE+" records in "+gap+" ms");
							
						} else if(gap >= EVENT_END_TIME_INTERVAL && inEvent) {
							inEvent = false;
							
							System.out.println("END event-id:"+ events +" rate: "+FIFO_SIZE+" records in "+gap+" ms");
							System.out.println();
						}
					}
				}
			}
		} finally{
			consumer.close();
		}
	}
}
