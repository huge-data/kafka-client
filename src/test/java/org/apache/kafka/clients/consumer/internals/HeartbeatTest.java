package org.apache.kafka.clients.consumer.internals;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.kafka.common.utils.MockTime;
import org.junit.Test;

/**
 * 用于协同处理的心跳测试
 *
 * @author wanggang
 *
 */
public class HeartbeatTest {

	private long timeout = 300L;
	private long interval = 100L;
	// 时间模拟器
	private MockTime time = new MockTime();
	// 心跳检测
	private Heartbeat heartbeat = new Heartbeat(timeout, interval, -1L);

	@Test
	public void testShouldHeartbeat() {
		heartbeat.sentHeartbeat(time.milliseconds());
		time.sleep((long) (interval * 1.1));
		assertTrue(heartbeat.shouldHeartbeat(time.milliseconds()));
	}

	@Test
	public void testShouldNotHeartbeat() {
		heartbeat.sentHeartbeat(time.milliseconds());
		time.sleep(interval / 2);
		assertFalse(heartbeat.shouldHeartbeat(time.milliseconds()));
	}

	@Test
	public void testTimeToNextHeartbeat() {
		heartbeat.sentHeartbeat(0);
		assertEquals(100, heartbeat.timeToNextHeartbeat(0));
		assertEquals(0, heartbeat.timeToNextHeartbeat(100));
		assertEquals(0, heartbeat.timeToNextHeartbeat(200));
	}

	@Test
	public void testSessionTimeoutExpired() {
		heartbeat.sentHeartbeat(time.milliseconds());
		time.sleep(305);
		assertTrue(heartbeat.sessionTimeoutExpired(time.milliseconds()));
	}

	@Test
	public void testResetSession() {
		heartbeat.sentHeartbeat(time.milliseconds());
		time.sleep(305);
		heartbeat.resetSessionTimeout(time.milliseconds());
		assertFalse(heartbeat.sessionTimeoutExpired(time.milliseconds()));
	}

}
