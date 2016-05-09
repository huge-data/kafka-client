package org.apache.kafka.clients.producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

import org.junit.Test;

/**
 * 生产者发送的记录（键值对）类测试
 *
 * @author wanggang
 *
 */
public class ProducerRecordTest {

	@Test
	public void testEqualsAndHashCode() {
		ProducerRecord<String, Integer> producerRecord = new ProducerRecord<>("test", 1, "key", 1);
		assertEquals(producerRecord, producerRecord);
		assertEquals(producerRecord.hashCode(), producerRecord.hashCode());

		ProducerRecord<String, Integer> equalRecord = new ProducerRecord<>("test", 1, "key", 1);
		assertEquals(producerRecord, equalRecord);
		assertEquals(producerRecord.hashCode(), equalRecord.hashCode());

		ProducerRecord<String, Integer> topicMisMatch = new ProducerRecord<>("test-1", 1, "key", 1);
		assertFalse(producerRecord.equals(topicMisMatch));

		ProducerRecord<String, Integer> partitionMismatch = new ProducerRecord<>("test", 2, "key",
				1);
		assertFalse(producerRecord.equals(partitionMismatch));

		ProducerRecord<String, Integer> keyMisMatch = new ProducerRecord<>("test", 1, "key-1", 1);
		assertFalse(producerRecord.equals(keyMisMatch));

		ProducerRecord<String, Integer> valueMisMatch = new ProducerRecord<>("test", 1, "key", 2);
		assertFalse(producerRecord.equals(valueMisMatch));

		ProducerRecord<String, Integer> nullFieldsRecord = new ProducerRecord<>("topic", null,
				null, null);
		assertEquals(nullFieldsRecord, nullFieldsRecord);
		assertEquals(nullFieldsRecord.hashCode(), nullFieldsRecord.hashCode());
	}

}
