package org.apache.kafka.clients.consumer.internals;

import java.util.List;
import java.util.Map;

import org.apache.kafka.common.TopicPartition;

/**
 * 模拟分区分配器实现类
 *
 * @author wanggang
 *
 */
public class MockPartitionAssignor extends AbstractPartitionAssignor {

	private Map<String, List<TopicPartition>> result = null;

	@Override
	public Map<String, List<TopicPartition>> assign(Map<String, Integer> partitionsPerTopic,
			Map<String, List<String>> subscriptions) {
		if (result == null)
			throw new IllegalStateException("Call to assign with no result prepared");
		return result;
	}

	@Override
	public String name() {
		return "consumer-mock-assignor";
	}

	public void clear() {
		this.result = null;
	}

	public void prepare(Map<String, List<TopicPartition>> result) {
		this.result = result;
	}

}
