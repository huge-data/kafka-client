package org.apache.kafka.common.metrics;

import java.util.List;
import java.util.Map;

/**
 * 隐含指标报告器
 *
 * @author wanggang
 *
 */
public class FakeMetricsReporter implements MetricsReporter {

	@Override
	public void configure(Map<String, ?> configs) {
	}

	@Override
	public void init(List<KafkaMetric> metrics) {
	}

	@Override
	public void metricChange(KafkaMetric metric) {
	}

	@Override
	public void metricRemoval(KafkaMetric metric) {
	}

	@Override
	public void close() {
	}

}
