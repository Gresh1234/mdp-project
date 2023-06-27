package org.mdp.kafka.cli;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.ArrayList;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.mdp.kafka.def.KafkaConstants;

public class VideoFilter {
	public static ArrayList<String> EARTHQUAKE_SUBSTRINGS = new ArrayList<String>();
	
	public static void main(String[] args) throws FileNotFoundException, IOException{
		
		if(args.length<3){
			   System.err.println("Usage [inputTopic] [outputTopic] [keyword1] [keyword2] ...");
			   return;
			  }
		
		Properties props = KafkaConstants.PROPS;

		// randomise consumer ID so messages cannot be read by another consumer
		//   (or at least it's more likely that a meteor wipes out life on Earth)
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		
		consumer.subscribe(Arrays.asList(args[0]));
		
		for(int i = 2; i< args.length; i++) {
			EARTHQUAKE_SUBSTRINGS.add(args[i]);
		}

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
							producer.send(new ProducerRecord<>(args[1], 0, record.timestamp(), record.key(), record.value()));
							// prevents multiple prints of the same tweet with multiple keywords
							break;
						}
					}
				}
			}
		} finally{
			consumer.close();
			producer.close();
		}
	}
}
