package org.apache.kafka.clients.consumer;

import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.test.MockMetricsReporter;
import org.junit.Assert;
import org.junit.Test;

/**
 * Kafka消费者对象测试
 *
 * @author wanggang
 *
 */
public class KafkaConsumerTest {

	private final String topic = "test";
	private final TopicPartition tp0 = new TopicPartition("test", 0);

	@Test
	public void testConstructorClose() throws Exception {
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testConstructorClose");
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
				"some.invalid.hostname.foo.bar:9999");
		props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
				MockMetricsReporter.class.getName());

		final int oldInitCount = MockMetricsReporter.INIT_COUNT.get();
		final int oldCloseCount = MockMetricsReporter.CLOSE_COUNT.get();
		try {
			KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props,
					new ByteArrayDeserializer(), new ByteArrayDeserializer());
			consumer.close();
		} catch (KafkaException e) {
			Assert.assertEquals(oldInitCount + 1, MockMetricsReporter.INIT_COUNT.get());
			Assert.assertEquals(oldCloseCount + 1, MockMetricsReporter.CLOSE_COUNT.get());
			Assert.assertEquals("Failed to construct kafka consumer", e.getMessage());
			return;
		}
		Assert.fail("should have caught an exception and returned");
	}

	@Test
	public void testSubscription() {
		Properties props = new Properties();
		props.setProperty(ConsumerConfig.CLIENT_ID_CONFIG, "testSubscription");
		props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9999");
		props.setProperty(ConsumerConfig.METRIC_REPORTER_CLASSES_CONFIG,
				MockMetricsReporter.class.getName());

		KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer<>(props,
				new ByteArrayDeserializer(), new ByteArrayDeserializer());

		consumer.subscribe(Collections.singletonList(topic));
		Assert.assertEquals(Collections.singleton(topic), consumer.subscription());
		Assert.assertTrue(consumer.assignment().isEmpty());

		consumer.subscribe(Collections.<String> emptyList());
		Assert.assertTrue(consumer.subscription().isEmpty());
		Assert.assertTrue(consumer.assignment().isEmpty());

		consumer.assign(Collections.singletonList(tp0));
		Assert.assertTrue(consumer.subscription().isEmpty());
		Assert.assertEquals(Collections.singleton(tp0), consumer.assignment());

		consumer.unsubscribe();
		Assert.assertTrue(consumer.subscription().isEmpty());
		Assert.assertTrue(consumer.assignment().isEmpty());

		consumer.close();
	}

}
