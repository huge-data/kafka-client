package org.apache.kafka.common.metrics;

import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.stats.Avg;
import org.apache.kafka.common.metrics.stats.Total;
import org.junit.Test;

/**
 * JMX报告器测试
 *
 * @author wanggang
 *
 */
public class JmxReporterTest {

	@Test
	public void testJmxRegistration() throws Exception {
		Metrics metrics = new Metrics();
		try {
			metrics.addReporter(new JmxReporter());
			Sensor sensor = metrics.sensor("kafka.requests");
			sensor.add(new MetricName("pack.bean1.avg", "grp1"), new Avg());
			sensor.add(new MetricName("pack.bean2.total", "grp2"), new Total());
			Sensor sensor2 = metrics.sensor("kafka.blah");
			sensor2.add(new MetricName("pack.bean1.some", "grp1"), new Total());
			sensor2.add(new MetricName("pack.bean2.some", "grp1"), new Total());
		} finally {
			metrics.close();
		}
	}

}
