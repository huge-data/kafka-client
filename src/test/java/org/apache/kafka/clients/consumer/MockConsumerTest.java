package org.apache.kafka.clients.consumer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;

import org.apache.kafka.clients.consumer.internals.NoOpConsumerRebalanceListener;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;

/**
 * 模拟消费者（MockConsumer）对象测试
 *
 * @author wanggang
 *
 */
public class MockConsumerTest {

	private MockConsumer<String, String> consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);

	@Test
	public void testSimpleMock() {
		consumer.subscribe(Arrays.asList("test"), new NoOpConsumerRebalanceListener());
		assertEquals(0, consumer.poll(1000).count());
		consumer.rebalance(Arrays.asList(new TopicPartition("test", 0), new TopicPartition("test",
				1)));
		// Mock consumers need to seek manually since they cannot automatically reset offsets
		HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
		beginningOffsets.put(new TopicPartition("test", 0), 0L);
		beginningOffsets.put(new TopicPartition("test", 1), 0L);
		consumer.updateBeginningOffsets(beginningOffsets);
		consumer.seek(new TopicPartition("test", 0), 0);
		ConsumerRecord<String, String> rec1 = new ConsumerRecord<>("test", 0, 0, "key1", "value1");
		ConsumerRecord<String, String> rec2 = new ConsumerRecord<>("test", 0, 1, "key2", "value2");
		consumer.addRecord(rec1);
		consumer.addRecord(rec2);
		ConsumerRecords<String, String> recs = consumer.poll(1);
		Iterator<ConsumerRecord<String, String>> iter = recs.iterator();
		assertEquals(rec1, iter.next());
		assertEquals(rec2, iter.next());
		assertFalse(iter.hasNext());
		assertEquals(2L, consumer.position(new TopicPartition("test", 0)));
		consumer.commitSync();
		assertEquals(2L, consumer.committed(new TopicPartition("test", 0)).offset());
	}

}
