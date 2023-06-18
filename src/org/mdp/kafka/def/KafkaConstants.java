package org.mdp.kafka.def;

import java.util.Properties;

import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

public class KafkaConstants {
	public static final Properties PROPS = new Properties();
	static {
		PROPS.put("bootstrap.servers", "c1:9092");
		PROPS.put("group.id", "default_consumer_group");
		PROPS.put("key.deserializer", StringDeserializer.class.getName());
		PROPS.put("value.deserializer", StringDeserializer.class.getName());
		PROPS.put("key.serializer", StringSerializer.class.getName());
		PROPS.put("value.serializer", StringSerializer.class.getName());
	}
}
