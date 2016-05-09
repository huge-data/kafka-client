package zx.soft.kafka.client.consumer;

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerRunner implements Runnable {

	private static Logger logger = LoggerFactory.getLogger(ConsumerRunner.class);

	private final AtomicBoolean closed = new AtomicBoolean(false);

	private static final int POLL_TIMEOUT = 1_000;

	private final Consumer<Object, Object> consumer;

	public ConsumerRunner(Consumer<Object, Object> consumer) {
		super();
		this.consumer = consumer;
	}

	@Override
	public void run() {
		try {
			while (!closed.get()) {
				ConsumerRecords<Object, Object> records = consumer.poll(POLL_TIMEOUT);
				logger.info("count: " + records.count());
				// 处理数据
				//				for (ConsumerRecord<Object, Object> record : records) {
				//					byte[] key = (byte[]) record.key();
				//					byte[] value = (byte[]) record.value();
				//					String keyStr = new String(key);
				//					String valueStr = new String(value);
				//					logger.info("offset = {}, key = {}, value = {}.", record.offset(),
				//							String.copyValueOf(keyStr.toCharArray(), 0, key.length),
				//							String.copyValueOf(valueStr.toCharArray(), 0, value.length));
				// 手动控制偏移量
				//				consumer.commitSync();
				//				}
			}
		} catch (WakeupException e) {
			logger.error("WakeupException: {}.", e);
			// 如果关闭的话，忽略该异常
			if (!closed.get()) {
				throw new RuntimeException(e);
			}
		} finally {
			consumer.close();
		}
	}

	// 关闭钩子（hook），其它线程可以调用
	public void shutdown() {
		closed.set(true);
		consumer.wakeup();
	}

}
