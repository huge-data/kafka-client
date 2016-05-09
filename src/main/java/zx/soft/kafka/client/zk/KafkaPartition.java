package zx.soft.kafka.client.zk;

import java.util.ArrayList;
import java.util.List;

public class KafkaPartition {

	private String id;
	private List<String> seeds = new ArrayList<>();

	public KafkaPartition(String id) {
		this.id = id;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public List<String> getSeeds() {
		return seeds;
	}

	public void setSeeds(List<String> seeds) {
		this.seeds = seeds;
	}

}
