package zx.soft.apt.mq.kafka.zookeeper;

import static org.junit.Assert.assertEquals;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.junit.Test;

import zx.soft.kafka.client.zk.OptionsBuilder;

public class OptionsBuilderTest {

	@Test
	public void testOptions() {
		Options options = OptionsBuilder.buildOptions();
		assertEquals(5, options.getOptions().size());
		Option topicOption = options.getOption("t");
		assertEquals("t", topicOption.getOpt());
		assertEquals("topic", topicOption.getLongOpt());
		assertEquals("topic", topicOption.getArgName());
		assertEquals("kafka topic", topicOption.getDescription());
		assertEquals(1, topicOption.getArgs());
	}

}
