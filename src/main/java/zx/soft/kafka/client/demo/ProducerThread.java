package zx.soft.kafka.client.demo;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

public class ProducerThread extends Thread {

	private final KafkaProducer<Integer, String> producer;
	private final String topic;
	private final Boolean isAsync;

	public ProducerThread(String topic, Boolean isAsync) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("client.id", "DemoProducer");
		props.put("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		producer = new KafkaProducer<>(props);
		this.topic = topic;
		this.isAsync = isAsync;
	}

	@Override
	public void run() {
		int messageNo = 1;
		while (true) {
			String messageStr = "Message_" + messageNo;
			long startTime = System.currentTimeMillis();
			if (isAsync) { // Send asynchronously
				producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageStr),
						new DemoCallBack(startTime, messageNo, messageStr));
			} else { // Send synchronously
				try {
					producer.send(new ProducerRecord<Integer, String>(topic, messageNo, messageStr))
							.get();
					System.out.println("Sent message: (" + messageNo + ", " + messageStr + ")");
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					e.printStackTrace();
				}
			}
			++messageNo;
		}
	}

}

class DemoCallBack implements Callback {

	private long startTime;
	private int key;
	private String message;

	public DemoCallBack(long startTime, int key, String message) {
		this.startTime = startTime;
		this.key = key;
		this.message = message;
	}

	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		long elapsedTime = System.currentTimeMillis() - startTime;
		if (metadata != null) {
			System.out.println("message(" + key + ", " + message + ") sent to partition("
					+ metadata.partition() + "), " + "offset(" + metadata.offset() + ") in "
					+ elapsedTime + " ms");
		} else {
			exception.printStackTrace();
		}
	}

}
