package zx.soft.kafka.client.consumer;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.kafka.client.pool.PoolConfig;
import zx.soft.kafka.client.pool.impl.ConsumerPool;
import zx.soft.utils.threads.ApplyThreadPool;

/**
 * 一个分区一个消费者线程
 *
 * 测试结果：
 *     1、2个核，线程池最大为2，阻塞队列为4, 就可以跑慢网卡；
 *     2、每个线程对应一个分区，完全可以消费完每个分区数据；
 *     3、建议每个线程读取多个分区；
 *
 * @author wanggang
 *
 */
public class ConsumerThreads {

	private static Logger logger = LoggerFactory.getLogger(ConsumerThreads.class);

	private String topic;
	private List<Integer> partitions = new ArrayList<>();
	private int partitionNumPerThread;
	private PoolConfig config;
	private Properties props;

	/**
	 * 主程序
	 */
	public static void main(String[] args) {
		//		String partitionsStr = "0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21,22,23";
		//		ConsumerThreads threads = new ConsumerThreads("apt-test", partitionsStr, 3);
		ConsumerThreads threads = new ConsumerThreads("apt-test", args[0],
				Integer.parseInt(args[1]));
		threads.run();
	}

	public ConsumerThreads(String topic, String partitionsStr, int partitionNumPerThread) {
		super();
		logger.info("Setting topic: {}, partions: {}.", topic, partitionsStr);
		this.topic = topic;
		for (String partition : partitionsStr.split(",")) {
			this.partitions.add(Integer.parseInt(partition));
		}
		// 每个线程读partitionNumPerThread分区
		this.partitionNumPerThread = partitionNumPerThread;
		if (this.partitions.size() % this.partitionNumPerThread != 0) {
			throw new RuntimeException(
					"partitions's size should be multiply of partitionNumPerThread!");
		}
		/* 对象池配置 */
		config = new PoolConfig();
		config.setMaxTotal(20);
		config.setMaxIdle(5);
		config.setMaxWaitMillis(1000);
		config.setTestOnBorrow(true);
		/* 参数配置 */
		props = new Properties();
		props.put("bootstrap.servers", "kafka01:9092,kafka02:9092,kafka03:9092");
		props.put("group.id", getHostName());
		props.put("enable.auto.commit", "true");
		props.put("auto.commit.interval.ms", "1000");
		props.put("session.timeout.ms", "30000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.ByteArrayDeserializer");
		props.put("value.deserializer",
				"org.apache.kafka.common.serialization.ByteArrayDeserializer");
	}

	public void run() {
		// 申请线程池
		logger.info("Starting ...");
		// 线程数
		int threadNum = this.partitions.size() / this.partitionNumPerThread;
		final ThreadPoolExecutor pool = ApplyThreadPool.getThreadPoolExector(threadNum);

		// 优雅关闭
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

			@Override
			public void run() {
				pool.shutdown();
			}

		}));

		/* 初始化对象池 */
		ConsumerPool consumerPool = new ConsumerPool(config, props);

		/* 加入线程执行 */
		for (int i = 0; i < threadNum; i++) {
			Consumer<Object, Object> consumer = consumerPool.getConnection();
			List<TopicPartition> partitionsList = new ArrayList<>();
			for (int j = 0; j < this.partitionNumPerThread; j++) {
				partitionsList.add(new TopicPartition(topic, partitions.get(i
						* partitionNumPerThread + j)));
			}
			// 指定分区
			consumer.assign(partitionsList);
			// 验证每个Consumer是否对应到制定分区
			//			Iterator<TopicPartition> it = consumer.assignment().iterator();
			//			while (it.hasNext()) {
			//				System.err.println(i + ": " + it.next().partition());
			//			}
			try {
				pool.execute(new ConsumerRunner(consumer));
			} catch (Exception e) {
				/* 对象返回到对象池 */
				consumerPool.returnConnection(consumer);
				logger.error("Exception: {}.", e);
				throw new RuntimeException(e);
			}
		}

		/* 关闭对象池 */
		consumerPool.close();

		// 关闭线程池
		pool.shutdown();
		try {
			pool.awaitTermination(60, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
		logger.info("Finished ...");
	}

	private String getHostName() {
		try {
			return InetAddress.getLocalHost().getHostName();
		} catch (UnknownHostException e) {
			return "UnknowHostName";
		}
	}

}
