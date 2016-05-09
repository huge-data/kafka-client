package org.apache.kafka.clients;

import java.util.Arrays;

import org.apache.kafka.common.config.ConfigException;
import org.junit.Test;

/**
 * 检查客户端输入的地址是否有效，格式：host:port列表
 *
 * @author wanggang
 *
 */
public class ClientUtilsTest {

	@Test
	public void testParseAndValidateAddresses() {
		check("127.0.0.1:8000");
		check("mydomain.com:8080");
		check("[::1]:8000");
		check("[2001:db8:85a3:8d3:1319:8a2e:370:7348]:1234", "mydomain.com:10000");
	}

	@Test(expected = ConfigException.class)
	public void testNoPort() {
		check("127.0.0.1");
	}

	private void check(String... url) {
		ClientUtils.parseAndValidateAddresses(Arrays.asList(url));
	}

}
