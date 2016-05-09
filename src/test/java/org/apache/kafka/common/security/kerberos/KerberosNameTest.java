package org.apache.kafka.common.security.kerberos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;

/**
 * 安全组件Kerbero名称类测试
 *
 * @author wanggang
 *
 */
public class KerberosNameTest {

	@Test
	public void testParse() throws IOException {
		List<String> rules = new ArrayList<>(Arrays.asList(
				"RULE:[1:$1](App\\..*)s/App\\.(.*)/$1/g", "RULE:[2:$1](App\\..*)s/App\\.(.*)/$1/g",
				"DEFAULT"));
		KerberosShortNamer shortNamer = KerberosShortNamer.fromUnparsedRules("REALM.COM", rules);

		KerberosName name = KerberosName.parse("App.service-name/example.com@REALM.COM");
		assertEquals("App.service-name", name.serviceName());
		assertEquals("example.com", name.hostName());
		assertEquals("REALM.COM", name.realm());
		assertEquals("service-name", shortNamer.shortName(name));

		name = KerberosName.parse("App.service-name@REALM.COM");
		assertEquals("App.service-name", name.serviceName());
		assertNull(name.hostName());
		assertEquals("REALM.COM", name.realm());
		assertEquals("service-name", shortNamer.shortName(name));

		name = KerberosName.parse("user/host@REALM.COM");
		assertEquals("user", name.serviceName());
		assertEquals("host", name.hostName());
		assertEquals("REALM.COM", name.realm());
		assertEquals("user", shortNamer.shortName(name));
	}

}
