package org.apache.kafka.common.record;

import static org.apache.kafka.common.utils.Utils.toArray;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

/**
 * 内存记录测试
 *
 * @author wanggang
 *
 */
@RunWith(value = Parameterized.class)
public class MemoryRecordsTest {

	private CompressionType compression;

	public MemoryRecordsTest(CompressionType compression) {
		this.compression = compression;
	}

	@Test
	public void testIterator() {
		MemoryRecords recs1 = MemoryRecords.emptyRecords(ByteBuffer.allocate(1024), compression);
		MemoryRecords recs2 = MemoryRecords.emptyRecords(ByteBuffer.allocate(1024), compression);
		List<Record> list = Arrays.asList(new Record("a".getBytes(), "1".getBytes()), new Record(
				"b".getBytes(), "2".getBytes()), new Record("c".getBytes(), "3".getBytes()));
		for (int i = 0; i < list.size(); i++) {
			Record r = list.get(i);
			recs1.append(i, r);
			recs2.append(i, toArray(r.key()), toArray(r.value()));
		}
		recs1.close();
		recs2.close();

		for (int iteration = 0; iteration < 2; iteration++) {
			for (MemoryRecords recs : Arrays.asList(recs1, recs2)) {
				Iterator<LogEntry> iter = recs.iterator();
				for (int i = 0; i < list.size(); i++) {
					assertTrue(iter.hasNext());
					LogEntry entry = iter.next();
					assertEquals(i, entry.offset());
					assertEquals(list.get(i), entry.record());
					entry.record().ensureValid();
				}
				assertFalse(iter.hasNext());
			}
		}
	}

	@Parameterized.Parameters
	public static Collection<Object[]> data() {
		List<Object[]> values = new ArrayList<>();
		for (CompressionType type : CompressionType.values())
			values.add(new Object[] { type });
		return values;
	}

}
