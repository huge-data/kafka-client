package zx.soft.kafka.client.demo;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * 测试单生产者对单分区写数据：
 * 1、同一个生产者往同一个分区写数据，写入什么顺序读取就是什么顺序；
 *
 * 测试环境：
 * 1、apt-test:24个分区，没有复制；
 * 2、apt-test1:1个分区，没有复制；
 *
 * @author wanggang
 *
 */
public class ProducerDemo {

	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "kafka01:19092,kafka02:19093,kafka03:19094");
		//		props.put("client.id", "ProducerDemo");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 81960);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		// Producer
		Producer<String, String> producer = new KafkaProducer<>(props);
		// 指定分区
		int partitionNum = 10;
		/* message */
		Boolean isAsync = Boolean.FALSE; // 是否异步
		for (int i = 0; i < 10000; i++) {
			if (isAsync) { // 发送异步消息
				producer.send(new ProducerRecord<String, String>("apt-test", partitionNum, i
						+ "hello", i + "world"), new DemoCallBack(System.currentTimeMillis(), i,
						Integer.toString(i)));
			} else { // 发送同步消息
				try {
					producer.send(
							new ProducerRecord<String, String>("apt-test", partitionNum, i
									+ "hello", i + "world")).get();
				} catch (InterruptedException | ExecutionException e) {
					e.printStackTrace();
				}
			}
		}
		//		producer.flush();
		/* close pool */
		producer.close();

		System.out.println("Finish!");
	}

	private static class DemoCallBack implements Callback {

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

}