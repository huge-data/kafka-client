package zx.soft.kafka.client.zk;

import java.util.List;

public class KafkaTopic {

	private String topic;
	private List<KafkaPartition> partitions;

	public KafkaTopic() {
	}

	public KafkaTopic(String topic, List<KafkaPartition> partitions) {
		this.topic = topic;
		this.partitions = partitions;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public List<KafkaPartition> getPartitions() {
		return partitions;
	}

	public void setPartitions(List<KafkaPartition> partitions) {
		this.partitions = partitions;
	}

}
