package zx.soft.kafka.client.zk;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;

/**
 * 创建参数集合
 *
 * @author wanggang
 *
 */
public class OptionsBuilder {

	public static Options buildOptions() {
		Options options = new Options();
		// -z  Zookeeper集群配置参数
		options.addOption(setOptionArg("zk", "zk-connect", "ZooKeeper connection String", "z"));
		// -t  topic参数
		options.addOption(setOptionArg("topic", "topic", "kafka topic", "t"));
		// -g  消费者组名参数
		options.addOption(setOptionArg("groupid", "consumer-group", "kafka consumer groupid", "g"));
		// -o  偏移量值
		options.addOption(setOptionArg("offset", "autooffset-reset", "Offset reset", "o"));
		// -l  Kafka限制值
		options.addOption(setOptionArg("limit", "limit", "kafka limit", "l"));

		return options;
	}

	private static Option setOptionArg(String argName, String longOpt, String description,
			String createName) {
		OptionBuilder.withArgName(argName);
		OptionBuilder.withLongOpt(longOpt);
		OptionBuilder.hasArg();
		OptionBuilder.withDescription(description);
		return OptionBuilder.create(createName);
	}

}
