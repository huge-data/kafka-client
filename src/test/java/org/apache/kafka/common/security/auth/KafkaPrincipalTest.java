package org.apache.kafka.common.security.auth;

import org.junit.Assert;
import org.junit.Test;

/**
 * Kafka重要信息类测试
 *
 * @author wanggang
 *
 */
public class KafkaPrincipalTest {

	@Test
	public void testPrincipalNameCanContainSeparator() {
		String name = "name" + KafkaPrincipal.SEPARATOR + "with" + KafkaPrincipal.SEPARATOR + "in"
				+ KafkaPrincipal.SEPARATOR + "it";

		KafkaPrincipal principal = KafkaPrincipal.fromString(KafkaPrincipal.USER_TYPE
				+ KafkaPrincipal.SEPARATOR + name);
		Assert.assertEquals(KafkaPrincipal.USER_TYPE, principal.getPrincipalType());
		Assert.assertEquals(name, principal.getName());
	}

	@Test
	public void testEqualsAndHashCode() {
		String name = "KafkaUser";
		KafkaPrincipal principal1 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, name);
		KafkaPrincipal principal2 = new KafkaPrincipal(KafkaPrincipal.USER_TYPE, name);

		Assert.assertEquals(principal1.hashCode(), principal2.hashCode());
		Assert.assertEquals(principal1, principal2);
	}

}
