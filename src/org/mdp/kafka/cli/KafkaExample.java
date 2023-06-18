package org.mdp.kafka.cli;

import java.time.Duration;
import java.util.Arrays;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.mdp.kafka.def.KafkaConstants;

public class KafkaExample {
	private final String topic;

	public KafkaExample(String topic) {
		this.topic = topic;
	}

	public void consume() {
		Properties props = KafkaConstants.PROPS;

		// randomise consumer ID so messages cannot be read by another consumer
		//  (or at least it's more likely that a meteor wipes out life on Earth)
		props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
		
		KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(props);
		consumer.subscribe(Arrays.asList(topic));
		try{
			while (true) {
				ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
				for (ConsumerRecord<String, String> record : records) {
					System.out.printf("%s [%d] offset=%d, timestamp=%d, key=%s, value=\"%s\"\n",
							record.topic(), record.partition(), record.offset(),
							record.timestamp(), record.key(), record.value());
				}
			}
		} finally{
			consumer.close();
		}
	}

	public void produce() {
		Thread one = new Thread() {
			public void run() {
				int partition = 0;
				
				Producer<String, String> producer = new KafkaProducer<String, String>(KafkaConstants.PROPS);
				try {
					int i = 0;
					while(true) {
						long timestamp = System.currentTimeMillis();
						String key = Integer.toString(i);
						String value = new Date().toString();
						
						producer.send(new ProducerRecord<String,String>(topic, partition, timestamp, key, value));
						Thread.sleep(1000);
						i++;
						
						// on the hour, write an extra record ...
						if(i % 3600 == 0) {
							producer.send(new ProducerRecord<String,String>(topic, partition, timestamp, "cuckoo", value));
						}
					}
				} catch (InterruptedException v) {
					System.err.println(v);
				} finally {
					producer.close();
				}
			}
		};
		one.start();
	}

	public static void main(String[] args) {
		if (args.length < 1) {
			System.err.println("Usage: [topicName]");
			return;
		}
		KafkaExample c = new KafkaExample(args[0]);
		c.produce();
		c.consume();
	}
}
