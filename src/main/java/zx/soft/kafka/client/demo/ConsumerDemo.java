package zx.soft.kafka.client.demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerDemo {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "kafka01:19092,kafka02:19093,kafka03:19094");
		props.put("group.id", "aaa");
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("apt-test"));
		for (int i = 0; i < 1000; i++) {
			ConsumerRecords<String, String> records = consumer.poll(100);
			System.err.println(i + ": " + records.count());
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("offset = %d, key = %s, value = %s", record.offset(),
						record.key(), record.value());
				System.out.println();
			}
		}
		consumer.close();

		System.err.println("Finish!");
	}

}
