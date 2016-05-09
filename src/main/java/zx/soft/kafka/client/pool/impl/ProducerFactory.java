package zx.soft.kafka.client.pool.impl;

import java.util.Properties;

import org.apache.commons.pool2.PooledObject;
import org.apache.commons.pool2.impl.DefaultPooledObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;

import zx.soft.kafka.client.pool.ConnectionFactory;

/**
 * Kafka连接工厂Producer实现类
 *
 * @author wanggang
 *
 */
class ProducerFactory implements ConnectionFactory<Producer<Object, Object>> {

	private static final long serialVersionUID = 8271607366818512399L;

	private final Properties config;

	/**
	 * 构造方法
	 *
	 * @param config 生产者配置
	 */
	public ProducerFactory(final Properties config) {
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
	public ProducerFactory(final String brokers, final int retries, final String acks,
			final String codec, final int batch) {
		Properties props = new Properties();
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
		props.put(ProducerConfig.RETRIES_CONFIG, 0);
		props.put(ProducerConfig.ACKS_CONFIG, acks);
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, codec);
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, codec);
		props.put(ProducerConfig.BATCH_SIZE_CONFIG, batch);
		// 默认参数
		props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
		props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);

		this.config = props;
	}

	@Override
	public PooledObject<Producer<Object, Object>> makeObject() throws Exception {
		Producer<Object, Object> producer = this.createConnection();
		return new DefaultPooledObject<>(producer);
	}

	@Override
	public void destroyObject(PooledObject<Producer<Object, Object>> p) throws Exception {
		Producer<Object, Object> producer = p.getObject();
		if (null != producer) {
			producer.close();
		}
	}

	@Override
	public boolean validateObject(PooledObject<Producer<Object, Object>> p) {
		Producer<Object, Object> producer = p.getObject();
		return (null != producer);
	}

	@Override
	public void activateObject(PooledObject<Producer<Object, Object>> p) throws Exception {
		// TODO
	}

	@Override
	public void passivateObject(PooledObject<Producer<Object, Object>> p) throws Exception {
		// TODO
	}

	@Override
	public Producer<Object, Object> createConnection() throws Exception {
		Producer<Object, Object> producer = new KafkaProducer<>(config);
		return producer;
	}

}
