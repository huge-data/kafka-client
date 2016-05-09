package zx.soft.kafka.client.pool.impl;

import java.util.Properties;

import org.apache.kafka.clients.producer.Producer;

import zx.soft.kafka.client.pool.ConnectionPool;
import zx.soft.kafka.client.pool.PoolBase;
import zx.soft.kafka.client.pool.PoolConfig;

/**
 * Kafka连接池
 *
 * @author wanggang
 *
 */
public class ProducerPool extends PoolBase<Producer<Object, Object>> implements
		ConnectionPool<Producer<Object, Object>> {

	private static final long serialVersionUID = -1506435964498488591L;

	private static final String LOCAL_MODE = "localhost:9092";

	/**
	 * 默认构造方法
	 */
	public ProducerPool() {
		this(LOCAL_MODE);
	}

	public ProducerPool(final String brokers) {
		this(new PoolConfig(), brokers);
	}

	public ProducerPool(final PoolConfig poolConfig, final String brokers) {
		this(poolConfig, brokers, 0);
	}

	public ProducerPool(final PoolConfig poolConfig, final String brokers, final int retries) {
		this(poolConfig, brokers, retries, "all",
				"org.apache.kafka.common.serialization.StringSerializer", 16384);
	}

	public ProducerPool(final PoolConfig poolConfig, final Properties props) {
		super(poolConfig, new ProducerFactory(props));
	}

	public ProducerPool(final PoolConfig poolConfig, final String brokers, final int retries,
			final String acks, final String codec, final int batch) {
		super(poolConfig, new ProducerFactory(brokers, retries, acks, codec, batch));
	}

	@Override
	public Producer<Object, Object> getConnection() {
		return super.getResource();
	}

	@Override
	public void returnConnection(Producer<Object, Object> conn) {
		super.returnResource(conn);
	}

	@Override
	public void invalidateConnection(Producer<Object, Object> conn) {
		super.invalidateResource(conn);
	}

}
