package zx.soft.kafka.client.pool.impl;

import java.util.Properties;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import zx.soft.kafka.client.pool.ConnectionFactory;

/**
 * Kafka连接工厂Consumer实现类
 *
 * @author wanggang
 *
 */
class ConsumerFactory implements ConnectionFactory<Consumer<Object, Object>> {

	private static final long serialVersionUID = 8271607366818512399L;

	private final Properties config;

	/**
	 * 构造方法
	 *
	 * @param config 生产者配置
	 */
	public ConsumerFactory(final Properties config) {
		this.config = config;
	}

	/**
	 * 构造方法
	 *
	 * @param brokers broker列表
	 * @param type 生产者类型
	 * @param acks 确认类型
	 * @param codec 压缩类型
	 * @param batch 批量大小
	 */
	public ConsumerFactory(final String brokers, final String groupId, final boolean autoCommit,
			final String codec, final long sessionTimeoutMs) {
		Properties props = new Properties();
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, autoCommit);
		props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, sessionTimeoutMs);
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, codec);
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, codec);
		// 默认
		props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

		this.config = props;
	}

	@Override
	public PooledObject<Consumer<Object, Object>> makeObject() throws Exception {
		Consumer<Object, Object> consumer = this.createConnection();
		return new DefaultPooledObject<>(consumer);
	}

	@Override
	public void destroyObject(PooledObject<Consumer<Object, Object>> p) throws Exception {
		Consumer<Object, Object> consumer = p.getObject();
		if (null != consumer) {
			consumer.close();
		}
	}

	@Override
	public boolean validateObject(PooledObject<Consumer<Object, Object>> p) {
		Consumer<Object, Object> consumer = p.getObject();
		return (null != consumer);
	}

	@Override
	public void activateObject(PooledObject<Consumer<Object, Object>> p) throws Exception {
		// TODO
	}

	@Override
	public void passivateObject(PooledObject<Consumer<Object, Object>> p) throws Exception {
		// TODO
	}

	@Override
	public Consumer<Object, Object> createConnection() throws Exception {
		Consumer<Object, Object> consumer = new KafkaConsumer<>(config);
		return consumer;
	}

}
