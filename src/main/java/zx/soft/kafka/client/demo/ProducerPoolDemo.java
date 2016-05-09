package zx.soft.kafka.client.demo;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.kafka.client.pool.PoolConfig;
import zx.soft.kafka.client.pool.impl.ProducerPool;

/**
 * Kakfa原生脚本测试命令：
 *    bin/kafka-producer-perf-test.sh --topic apt-test --num-records 50000000
 *      --record-size 10000 --throughput 10000000 --producer-props bootstrap.servers=kafka01:9092,kafka02:9092,kafka03:9092
 *       acks=0 batch.size=81960
 * 观察Topic生产者发送的数据命令：
 *    bin/kafka-console-consumer.sh --zookeeper kafka01:2181,kafka02:2181,kafka03:2181/kafka
 *      --from-beginning --topic apt-test
 * 测试结果：
 *     千兆网卡，单台机器可达到117M/s，基本可以达到网卡极限。
 *
 * @author wanggang
 *
 */
public class ProducerPoolDemo {

	private static Logger logger = LoggerFactory.getLogger(ProducerPoolDemo.class);

	public static void main(String[] args) {
		/* 对象池配置 */
		PoolConfig config = new PoolConfig();
		config.setMaxTotal(20);
		config.setMaxIdle(5);
		config.setMaxWaitMillis(1000);
		config.setTestOnBorrow(true);

		/* 参数配置 */
		Properties props = new Properties();
		props.put("bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092");
		props.put("acks", "0");
		props.put("retries", 0);
		props.put("batch.size", 81960);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");

		/* 初始化对象池 */
		ProducerPool pool = new ProducerPool(config, props);

		/* 从对象池获取对象 */
		Producer<Object, Object> producer = pool.getConnection();

		/* 发送消息设置 */
		int recordSize = 10_000;
		byte[] payload = new byte[recordSize];
		Arrays.fill(payload, (byte) 1);
		/* 发送消息 */
		int numRecords = 50_000_000;
		for (int i = 0; i < numRecords; i++) {
			//			System.err.println(i);
			ProducerRecord<Object, Object> record = new ProducerRecord<Object, Object>("apt-test",
					("apt-key-" + i).getBytes(), payload); // ("apt-value-" + i).getBytes()
			producer.send(record, new ProducerCallback(i));
		}

		/* 对象返回到对象池 */
		pool.returnConnection(producer);

		/* 关闭对象池 */
		pool.close();

		System.out.println("Finish!");
	}

	public static class ProducerCallback implements Callback {

		private int index;

		public ProducerCallback(int index) {
			this.index = index;
		}

		@Override
		public void onCompletion(RecordMetadata metadata, Exception exception) {
			if (exception != null) {
				logger.error("Producing {} message failed: {}", index, exception);
			}
		}

	}

}
