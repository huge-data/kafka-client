package org.apache.kafka.clients.consumer.internals;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * 同步请求结果集合类测试
 *
 * @author wanggang
 *
 */
public class RequestFutureTest {

	@Test
	public void testComposeSuccessCase() {
		RequestFuture<String> future = new RequestFuture<>();
		RequestFuture<Integer> composed = future
				.compose(new RequestFutureAdapter<String, Integer>() {

					@Override
					public void onSuccess(String value, RequestFuture<Integer> future) {
						future.complete(value.length());
					}

				});

		future.complete("hello");

		assertTrue(composed.isDone());
		assertTrue(composed.succeeded());
		assertEquals(5, (int) composed.value());
	}

	@Test
	public void testComposeFailureCase() {
		RequestFuture<String> future = new RequestFuture<>();
		RequestFuture<Integer> composed = future
				.compose(new RequestFutureAdapter<String, Integer>() {

					@Override
					public void onSuccess(String value, RequestFuture<Integer> future) {
						future.complete(value.length());
					}

				});

		RuntimeException e = new RuntimeException();
		future.raise(e);

		assertTrue(composed.isDone());
		assertTrue(composed.failed());
		assertEquals(e, composed.exception());
	}

}
