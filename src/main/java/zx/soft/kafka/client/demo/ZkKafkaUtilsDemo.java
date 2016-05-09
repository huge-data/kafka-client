package zx.soft.kafka.client.demo;

import java.io.IOException;
import java.util.List;

import zx.soft.kafka.client.zk.KafkaPartition;
import zx.soft.kafka.client.zk.KafkaTopic;
import zx.soft.kafka.client.zk.ZkKafkaUtils;
import zx.soft.utils.json.JsonUtils;

public class ZkKafkaUtilsDemo {

	/**
	 * 测试函数
	 */
	public static void main(String[] args) throws IOException {
		ZkKafkaUtils zkUtils = new ZkKafkaUtils("kafka01:2181,kafka02:2181,kafka03:2181");
		//		 查看某个目录下面的子名称
		List<String> childNames = zkUtils.getChildrenNames("/kafka/consumers");
		System.out.println(childNames);
		// 获取Kafka的根目录名称
		List<String> rootNames = zkUtils.getKafkaRootNames();
		System.out.println(rootNames);
		// 获取所有的Topic名称
		List<String> topicNames = zkUtils.getTopicNames();
		System.out.println(topicNames);
		// 获取主题详细信息
		KafkaTopic topic = zkUtils.getKafkaTopic("apt-cache-rep");
		System.out.println(JsonUtils.toJson(topic));
		// 根据Kafka节点Id获取节点信息
		String broker = zkUtils.getBroker("1");
		System.out.println(broker);
		// 获取某个主题的分区详细信息
		List<KafkaPartition> topicPattitions = zkUtils.getPartitions("apt-cache-rep");
		System.out.println(JsonUtils.toJson(topicPattitions));

		zkUtils.close();
	}

}
