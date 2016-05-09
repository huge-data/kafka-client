package zx.soft.kafka.client.zk;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.exception.ZkNoNodeException;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import zx.soft.utils.log.LogbackUtil;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Zookeeper关于Kafka数据操作工具类
 *
 * @author wanggang
 *
 */
public class ZkKafkaUtils implements Closeable {

	private static Logger logger = LoggerFactory.getLogger(ZkKafkaUtils.class);

	// 服务端部署Kakfa时设置Zookeeper中的目录，一般为空，我们这里设置了kafka作为目录
	private static final String KAFKA_ROOT = "/kafka";

	// 消费者子目录
	private static final String CONSUMERS_PATH = KAFKA_ROOT + "/consumers";
	// 节点子目录
	private static final String BROKER_PATH = KAFKA_ROOT + "/brokers";
	// 节点下面的Id目录
	private static final String BROKER_IDS_PATH = BROKER_PATH + "/ids";
	// 节点下面的主题目录
	private static final String BROKER_TOPICS_PATH = BROKER_PATH + "/topics";

	private ZkClient zkClient;
	private Map<String, String> brokers;

	public ZkKafkaUtils(String zkServers) {
		zkClient = new ZkClient(zkServers, 10000, 10000, new StringSerializer());
	}

	/**
	 * 获取某个目录子名称
	 *
	 * @return
	 */
	public List<String> getChildrenNames(String path) {
		List<String> data = zkClient.getChildren(path);
		return data;
	}

	/**
	 * 获取Kafka的根目录名称
	 *
	 * @return
	 */
	public List<String> getKafkaRootNames() {
		List<String> data = zkClient.getChildren(KAFKA_ROOT);
		return data;
	}

	/**
	 * 获取所有的Topic名称
	 *
	 * @return
	 */
	public List<String> getTopicNames() {
		List<String> data = zkClient.getChildren(BROKER_TOPICS_PATH);
		return data;
	}

	/**
	 * 获取主题详细信息
	 *
	 * @param topic  主题名
	 * @return
	 */
	public KafkaTopic getKafkaTopic(String topic) {
		KafkaTopic kafkaTopic = new KafkaTopic();
		kafkaTopic.setTopic(topic);
		kafkaTopic.setPartitions(this.getPartitions(topic));

		return kafkaTopic;
	}

	/**
	 * 根据Kafka节点Id获取节点信息
	 *
	 * @param id  节点id
	 * @return
	 */
	public String getBroker(String id) {
		if (brokers == null) {
			brokers = new HashMap<>();
			// 获取所有子节点id信息列表，如：[0, 1, 2]
			List<String> brokerIds = getChildrenParentMayNotExist(BROKER_IDS_PATH);
			for (String bid : brokerIds) {
				// 获取某个子节点的详细信息，如：
				// {"jmx_port":-1,"timestamp":"1451400605994","endpoints":["PLAINTEXT://kafka01:9092"],"host":"kafka01","version":2,"port":9092}
				String data = zkClient.<String> readData(BROKER_IDS_PATH + "/" + bid);
				try {
					ObjectMapper objectMapper = new ObjectMapper();
					JsonNode rootNode = objectMapper.readValue(data, JsonNode.class);
					JsonNode hostNode = rootNode.path("host");
					JsonNode portNode = rootNode.path("port");
					brokers.put(bid, hostNode.textValue() + ":" + portNode.intValue());
				} catch (Exception e) {
					logger.error("Exception: {}.", LogbackUtil.expection2Str(e));
				}
			}
		}

		return brokers.get(id);
	}

	/**
	 * 获取某个主题的分区详细信息
	 *
	 * 注意：返回的seed节点信息（host:port）列表中，第一个节点就是Leader节点
	 *
	 * @param topic  主题名
	 * @return
	 */
	public List<KafkaPartition> getPartitions(String topic) {
		List<KafkaPartition> partitions = new ArrayList<>();
		try {
			// 获取某个主题的详细信息，如：
			// {"version":1,"partitions":{"12":[2],"8":[1],"19":[0],"23":[1],"4":[0],"15":[2],"11":[1],"9":[2],"22":[0],"13":[0],"16":[0],"5":[1],"10":[0],"21":[2],"6":[2],"1":[0],"17":[1],"14":[1],"0":[2],"20":[1],"2":[1],"18":[2],"7":[0],"3":[2]}}
			String topicData = zkClient.readData(BROKER_TOPICS_PATH + "/" + topic);

			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode rootNode = objectMapper.readValue(topicData, JsonNode.class);
			JsonNode partitionsNode = rootNode.path("partitions");
			for (Iterator<Entry<String, JsonNode>> iterator = partitionsNode.fields(); iterator
					.hasNext();) {
				Entry<String, JsonNode> partitionNode = iterator.next();
				KafkaPartition partition = new KafkaPartition(partitionNode.getKey());
				for (Iterator<JsonNode> iterator2 = partitionNode.getValue().iterator(); iterator2
						.hasNext();) {
					JsonNode brokerNode = iterator2.next();
					String broker = this.getBroker(brokerNode.asText());
					if (broker != null) {
						partition.getSeeds().add(broker);
					}
				}
				partitions.add(partition);
			}
		} catch (Exception e) {
			logger.error("Exception: {}.", LogbackUtil.expection2Str(e));
		}

		return partitions;
	}

	/**
	 * 提交数据
	 *
	 * @param group      组名
	 * @param topic      主题名
	 * @return
	 */
	public boolean commit(String group, String topic) {
		List<String> partitions = getChildrenParentMayNotExist(getTempOffsetsPath(group, topic));
		for (String partition : partitions) {
			String path = getTempOffsetsPath(group, topic, partition);
			String offset = this.zkClient.readData(path);
			setLastCommit(group, topic, partition, Long.valueOf(offset), false);
			this.zkClient.delete(path);
		}

		return true;
	}

	/**
	 * 获取最新的提交
	 *
	 * @param group      组名
	 * @param topic      主题名
	 * @param partition  分区号
	 * @return
	 */
	public long getLastCommit(String group, String topic, String partition) {
		String znode = getOffsetsPath(group, topic, partition);
		String offset = this.zkClient.readData(znode, true);

		if (offset == null) {
			return 0L;
		}

		return Long.valueOf(offset) + 1;
	}

	/**
	 * 设置最新提交
	 *
	 * @param group      组名
	 * @param topic      主题名
	 * @param partition  分区号
	 * @param commit     提交值
	 * @param temp       是否采用临时偏移量
	 */
	public void setLastCommit(String group, String topic, String partition, long commit,
			boolean temp) {
		String path = temp ? getTempOffsetsPath(group, topic, partition) : getOffsetsPath(group,
				topic, partition);
		if (!zkClient.exists(path)) {
			zkClient.createPersistent(path, true);
		}
		zkClient.writeData(path, commit);
	}

	/**
	 * 获取该目录下面的所有子目录名称信息
	 *
	 * @param path  绝对路径名
	 * @return
	 */
	private List<String> getChildrenParentMayNotExist(String path) {
		try {
			List<String> children = zkClient.getChildren(path);
			return children;
		} catch (ZkNoNodeException e) {
			return new ArrayList<String>();
		}
	}

	private String getOffsetsPath(String group, String topic, String partition) {
		return CONSUMERS_PATH + "/" + group + "/offsets/" + topic + "/" + partition;
	}

	private String getTempOffsetsPath(String group, String topic, String partition) {
		return CONSUMERS_PATH + "/" + group + "/offsets-temp/" + topic + "/" + partition;
	}

	private String getTempOffsetsPath(String group, String topic) {
		return CONSUMERS_PATH + "/" + group + "/offsets-temp/" + topic;
	}

	@Override
	public void close() throws IOException {
		if (zkClient != null) {
			zkClient.close();
		}
	}

	/**
	 * ZkSerializer的String序列化实现
	 *
	 * @author wanggang
	 *
	 */
	private static class StringSerializer implements ZkSerializer {

		public StringSerializer() {
		}

		@Override
		public Object deserialize(byte[] data) throws ZkMarshallingError {
			if (data == null)
				return null;
			return new String(data);
		}

		@Override
		public byte[] serialize(Object data) throws ZkMarshallingError {
			return data.toString().getBytes();
		}

	}

}
