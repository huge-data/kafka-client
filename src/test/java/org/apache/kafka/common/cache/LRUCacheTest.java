package org.apache.kafka.common.cache;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

import org.junit.Test;

/**
 * “最近使用”缓存算法测试
 *
 * @author wanggang
 *
 */
public class LRUCacheTest {

	@Test
	public void testPutGet() {
		Cache<String, String> cache = new LRUCache<>(4);

		cache.put("a", "b");
		cache.put("c", "d");
		cache.put("e", "f");
		cache.put("g", "h");

		assertEquals(4, cache.size());

		assertEquals("b", cache.get("a"));
		assertEquals("d", cache.get("c"));
		assertEquals("f", cache.get("e"));
		assertEquals("h", cache.get("g"));
	}

	@Test
	public void testRemove() {
		Cache<String, String> cache = new LRUCache<>(4);

		cache.put("a", "b");
		cache.put("c", "d");
		cache.put("e", "f");
		assertEquals(3, cache.size());

		assertEquals(true, cache.remove("a"));
		assertEquals(2, cache.size());
		assertNull(cache.get("a"));
		assertEquals("d", cache.get("c"));
		assertEquals("f", cache.get("e"));

		assertEquals(false, cache.remove("key-does-not-exist"));

		assertEquals(true, cache.remove("c"));
		assertEquals(1, cache.size());
		assertNull(cache.get("c"));
		assertEquals("f", cache.get("e"));

		assertEquals(true, cache.remove("e"));
		assertEquals(0, cache.size());
		assertNull(cache.get("e"));
	}

	@Test
	public void testEviction() {
		Cache<String, String> cache = new LRUCache<>(2);

		cache.put("a", "b");
		cache.put("c", "d");
		assertEquals(2, cache.size());

		cache.put("e", "f");
		assertEquals(2, cache.size());
		assertNull(cache.get("a"));
		assertEquals("d", cache.get("c"));
		assertEquals("f", cache.get("e"));

		// Validate correct access order eviction
		cache.get("c");
		cache.put("g", "h");
		assertEquals(2, cache.size());
		assertNull(cache.get("e"));
		assertEquals("d", cache.get("c"));
		assertEquals("h", cache.get("g"));
	}

}
