package org.mdp.kafka.cli;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.mdp.kafka.def.KafkaConstants;

public class PrintEarthquakeTweets {
	public static final String[] EARTHQUAKE_SUBSTRINGS = new String[] { "terremoto", "temblor", "sismo", "quake" };
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
		Properties props = KafkaConstants.PROPS;

		// randomise consumer ID so messages cannot be read by another consumer
		//   (or at least it's more likely that a meteor wipes out life on Earth)
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		
		consumer.subscribe(Arrays.asList(args[0]));
		
		try{
			while (true) {
				// every ten milliseconds get all records in a batch
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10));
				
				// for all records in the batch
				for (ConsumerRecord<String, String> record : records) {
					String lowercase = record.value().toLowerCase();
					
					// check if record value contains keyword
					// (could be optimised a lot)
					for(String ek: EARTHQUAKE_SUBSTRINGS){
						// if so print it out to the console
						if(lowercase.contains(ek)){
							System.out.println(record.value());
							// prevents multiple prints of the same tweet with multiple keywords
							break;
						}
					}
				}
			}
		} finally{
			consumer.close();
		}
	}
}
