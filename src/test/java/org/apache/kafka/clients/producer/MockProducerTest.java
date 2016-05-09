package org.apache.kafka.clients.producer;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import org.apache.kafka.clients.producer.internals.DefaultPartitioner;
import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.test.MockSerializer;
import org.junit.Test;

/**
 * 模拟生产者测试
 *
 * @author wanggang
 *
 */
public class MockProducerTest {

	private String topic = "topic";

	@Test
	public void testAutoCompleteMock() throws Exception {
		MockProducer<byte[], byte[]> producer = new MockProducer<>(true, new MockSerializer(),
				new MockSerializer());
		ProducerRecord<byte[], byte[]> record = new ProducerRecord<>(topic, "key".getBytes(),
				"value".getBytes());
		Future<RecordMetadata> metadata = producer.send(record);
		assertTrue("Send should be immediately complete", metadata.isDone());
		assertFalse("Send should be successful", isError(metadata));
		assertEquals("Offset should be 0", 0L, metadata.get().offset());
		assertEquals(topic, metadata.get().topic());
		assertEquals("We should have the record in our history", singletonList(record),
				producer.history());
		producer.clear();
		assertEquals("Clear should erase our history", 0, producer.history().size());
		producer.close();
	}

	@Test
	public void testPartitioner() throws Exception {
		PartitionInfo partitionInfo0 = new PartitionInfo(topic, 0, null, null, null);
		PartitionInfo partitionInfo1 = new PartitionInfo(topic, 1, null, null, null);
		Cluster cluster = new Cluster(new ArrayList<Node>(0),
				asList(partitionInfo0, partitionInfo1), Collections.<String> emptySet());
		MockProducer<String, String> producer = new MockProducer<>(cluster, true,
				new DefaultPartitioner(), new StringSerializer(), new StringSerializer());
		ProducerRecord<String, String> record = new ProducerRecord<>(topic, "key", "value");
		Future<RecordMetadata> metadata = producer.send(record);
		assertEquals("Partition should be correct", 1, metadata.get().partition());
		producer.clear();
		assertEquals("Clear should erase our history", 0, producer.history().size());
		producer.close();
	}

	@Test
	public void testManualCompletion() throws Exception {
		MockProducer<byte[], byte[]> producer = new MockProducer<>(false, new MockSerializer(),
				new MockSerializer());
		ProducerRecord<byte[], byte[]> record1 = new ProducerRecord<>(topic, "key1".getBytes(),
				"value1".getBytes());
		ProducerRecord<byte[], byte[]> record2 = new ProducerRecord<>(topic, "key2".getBytes(),
				"value2".getBytes());
		Future<RecordMetadata> md1 = producer.send(record1);
		assertFalse("Send shouldn't have completed", md1.isDone());
		Future<RecordMetadata> md2 = producer.send(record2);
		assertFalse("Send shouldn't have completed", md2.isDone());
		assertTrue("Complete the first request", producer.completeNext());
		assertFalse("Requst should be successful", isError(md1));
		assertFalse("Second request still incomplete", md2.isDone());
		IllegalArgumentException e = new IllegalArgumentException("blah");
		assertTrue("Complete the second request with an error", producer.errorNext(e));
		try {
			md2.get();
			fail("Expected error to be thrown");
		} catch (ExecutionException err) {
			assertEquals(e, err.getCause());
		}
		assertFalse("No more requests to complete", producer.completeNext());

		Future<RecordMetadata> md3 = producer.send(record1);
		Future<RecordMetadata> md4 = producer.send(record2);
		assertTrue("Requests should not be completed.", !md3.isDone() && !md4.isDone());
		producer.flush();
		assertTrue("Requests should be completed.", md3.isDone() && md4.isDone());

		producer.close();
	}

	private boolean isError(Future<?> future) {
		try {
			future.get();
			return false;
		} catch (Exception e) {
			return true;
		}
	}

}
