package org.apache.kafka.common.security.ssl;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Map;

import javax.net.ssl.SSLEngine;

import org.apache.kafka.common.network.Mode;
import org.apache.kafka.test.TestSslUtils;
import org.junit.Test;

/**
 * SSL工厂和选择器测试
 *
 * 测试使用了简单的Socket服务器，并将请求数据原样返回。
 *
 * @author wanggang
 *
 */
public class SslFactoryTest {

	@Test
	public void testSslFactoryConfiguration() throws Exception {
		File trustStoreFile = File.createTempFile("truststore", ".jks");
		Map<String, Object> serverSslConfig = TestSslUtils.createSslConfig(false, true,
				Mode.SERVER, trustStoreFile, "server");
		SslFactory sslFactory = new SslFactory(Mode.SERVER);
		sslFactory.configure(serverSslConfig);
		//host and port are hints
		SSLEngine engine = sslFactory.createSslEngine("localhost", 0);
		assertNotNull(engine);
		String[] expectedProtocols = { "TLSv1.2" };
		assertArrayEquals(expectedProtocols, engine.getEnabledProtocols());
		assertEquals(false, engine.getUseClientMode());
	}

	@Test
	public void testClientMode() throws Exception {
		File trustStoreFile = File.createTempFile("truststore", ".jks");
		Map<String, Object> clientSslConfig = TestSslUtils.createSslConfig(false, true,
				Mode.CLIENT, trustStoreFile, "client");
		SslFactory sslFactory = new SslFactory(Mode.CLIENT);
		sslFactory.configure(clientSslConfig);
		//host and port are hints
		SSLEngine engine = sslFactory.createSslEngine("localhost", 0);
		assertTrue(engine.getUseClientMode());
	}

}
