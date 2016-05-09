package zx.soft.kafka.client.driver;

import zx.soft.kafka.client.consumer.ConsumerThreads;
import zx.soft.kafka.client.demo.ConsumerDemo;
import zx.soft.kafka.client.demo.ConsumerPoolDemo;
import zx.soft.kafka.client.demo.ProducerDemo;
import zx.soft.kafka.client.demo.ProducerPoolDemo;
import zx.soft.utils.driver.ProgramDriver;

/**
 * 驱动类
 *
 * @author wanggang
 *
 */
public class KafkaClientDriver {

	/**
	 * 主函数
	 */
	public static void main(String[] args) {

		int exitCode = -1;
		ProgramDriver pgd = new ProgramDriver();
		try {
			pgd.addClass("producerDemo", ProducerDemo.class, "单线程生产者测试");
			pgd.addClass("consumerDemo", ConsumerDemo.class, "单线程消费者测试");
			pgd.addClass("producerPoolDemo", ProducerPoolDemo.class, "基于对象池的单线程生产者测试");
			pgd.addClass("consumerPoolDemo", ConsumerPoolDemo.class, "基于对象池的单线程消费者测试");
			pgd.addClass("consumerThreads", ConsumerThreads.class, "基于对象池的多线程消费者测试");
			pgd.driver(args);
			// Success
			exitCode = 0;
		} catch (Throwable e) {
			throw new RuntimeException(e);
		}

		System.exit(exitCode);

	}

}
