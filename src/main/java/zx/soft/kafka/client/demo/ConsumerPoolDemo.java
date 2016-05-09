package zx.soft.kafka.client.demo;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.kafka.client.pool.PoolConfig;
import zx.soft.kafka.client.pool.impl.ConsumerPool;

/**
 * 自动的偏移量提交，实时的获取生产数据
 *
 * 参考资料：
 * http://kafka.apache.org/090/javadoc/index.html?org/apache/kafka/clients/consumer/KafkaConsumer.html
 *
 * @author wanggang
 *
 */
public class ConsumerPoolDemo {

	private static Logger logger = LoggerFactory.getLogger(ConsumerPoolDemo.class);

	public static void main(String[] args) {
		/* 对象池配置 */
		PoolConfig config = new PoolConfig();
		config.setMaxTotal(20);
		config.setMaxIdle(5);
		config.setMaxWaitMillis(1000);
		config.setTestOnBorrow(true);

		/* 参数配置 */
		Properties props = new Properties();
		// 不需要制定完整的kafka集群列表，系统会自动寻找可用节点
		props.put("bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092");
		props.put("group.id", "wgybzb");
		// Setting enable.auto.commit means that offsets are committed automatically
		// with a frequency controlled by the config auto.commit.interval.ms.
		props.put("enable.auto.commit", "true");
		// The broker will automatically detect failed processes in the test group by
		// using a heartbeat mechanism. The consumer will automatically ping the cluster
		// periodically, which lets the cluster know that it is alive. As long as the
		// consumer is able to do this it is considered alive and retains the right to
		// consume from the partitions assigned to it. If it stops heartbeating for a
		// period of time longer than session.timeout.ms then it will be considered dead
		// and its partitions will be assigned to another process.
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.ByteArrayDeserializer");

		/* 初始化对象池 */
		ConsumerPool pool = new ConsumerPool(config, props);

		/* 从对象池获取对象 */
		Consumer<Object, Object> consumer = pool.getConnection();

		/* 指定分区 */
		String topic = "apt-test";
		//		List<TopicPartition> partitions = Arrays.asList(//
		//				new TopicPartition(topic, 0), //
		//				new TopicPartition(topic, 2));
		//		consumer.assign(partitions);

		/* 指定分区位置 */
		//		consumer.seek(new TopicPartition(topic, 10), 10_000);

		/* 消费消息设置 */
		// 指定分区的时候就不需要下面这句
		consumer.subscribe(Arrays.asList(topic));
		for (int i = 0; i < 10_000; i++) {
			ConsumerRecords<Object, Object> records = consumer.poll(100);
			System.out.println(i + ": " + records.count());
			for (ConsumerRecord<Object, Object> record : records) {
				byte[] key = (byte[]) record.key();
				byte[] value = (byte[]) record.value();
				String keyStr = new String(key);
				String valueStr = new String(value);
				System.out.printf("offset = %d, key = %s, value = %s", record.offset(),
						String.copyValueOf(keyStr.toCharArray(), 0, key.length),
						String.copyValueOf(valueStr.toCharArray(), 0, value.length));
				System.out.println();
				// 手动控制偏移量
				//				consumer.commitSync();
			}

		}

		/* 对象返回到对象池 */
		pool.returnConnection(consumer);

		/* 关闭对象池 */
		pool.close();

		System.out.println("Finish!");
	}

	public static class RebalanceCallback implements ConsumerRebalanceListener {

		@Override
		public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
			logger.info("Rebalance callback, revoked: {}", partitions);
		}

		@Override
		public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
			logger.info("Rebalance callback, assigned: {}", partitions);
		}

	}

}
