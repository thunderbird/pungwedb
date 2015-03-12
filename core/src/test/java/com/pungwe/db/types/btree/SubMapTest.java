/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.pungwe.db.types.btree;

import com.pungwe.db.io.serializers.DBObjectSerializer;
import com.pungwe.db.io.serializers.LZ4Serializer;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.serializers.Serializers;
import com.pungwe.db.io.store.AppendOnlyStore;
import com.pungwe.db.io.store.InstanceCachingStore;
import com.pungwe.db.io.store.Store;
import com.pungwe.db.io.volume.MemoryVolume;
import com.pungwe.db.io.volume.Volume;
import com.pungwe.db.types.BasicDBObject;
import com.pungwe.db.types.DBObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Comparator;
import java.util.concurrent.ConcurrentNavigableMap;

import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

/**
 * Created by ian on 10/03/2015.
 */
public class SubMapTest {

	private static final Comparator<Long> comp = new Comparator<Long>() {

		@Override
		public int compare(Long o1, Long o2) {
			if (o1 == null) {
				return -1;
			}
			if (o2 == null) {
				return 1;
			}
			return o1.compareTo(o2);
		}
	};

	private static Serializer<Long> keySerializer = new Serializers.NUMBER();
	private static Serializer<DBObject> valueSerializer = new LZ4Serializer<>(new DBObjectSerializer());
	private BTreeMap<Long, Long> tree;

	@Before
	public void setupTests() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		AppendOnlyStore store = new AppendOnlyStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		tree = addManyBulkSingleThread(cacheStore, 1000, 100, volume);
	}

	@After
	public void tearDownTests() {
		tree = null;
	}
	@Test
	public void testGetSubMapDefault() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, 713l);
		assertEquals(50l, (long)sub.firstKey());
		assertEquals(712l, (long)sub.lastKey());
	}


	@Test
	public void testGetSubMapNonInclusive() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, false, 713l, false);
		assertEquals(51l, (long)sub.firstKey());
		assertEquals(712l, (long)sub.lastKey());
	}

	@Test
	public void testGetSubMapInclusive() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 713l, true);
		assertEquals(50l, (long)sub.firstKey());
		assertEquals(713l, (long)sub.lastKey());
	}

	@Test
	public void testGetSubMapLoInclusive() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 713l, false);
		assertEquals(50l, (long)sub.firstKey());
		assertEquals(712l, (long)sub.lastKey());
	}

	@Test
	public void testGetSubMapHiInclusive() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, false, 713l, true);
		assertEquals(51l, (long)sub.firstKey());
		assertEquals(713l, (long)sub.lastKey());
	}

	@Test
	public void testLowerEntry() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 713l, true);
		assertNull(sub.lowerKey(50l));
		assertEquals(51l, (long)sub.lowerKey(52l));
	}

	@Test
	public void testFloorEntry() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 713l, true);
		assertEquals(50l, (long)sub.floorKey(50l));
		assertEquals(51l, (long)sub.floorKey(52l));
	}

	@Test
	public void testHigherEntry() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 713l, true);
		assertNull(sub.higherKey(713l));
		assertEquals(52l, (long)sub.higherKey(51l));
	}

	@Test
	public void testCeilingEntry() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 713l, true);
		assertEquals(713l, (long)sub.ceilingKey(713l));
		assertEquals(713l, (long)sub.ceilingKey(712l));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testLowerKeyTooLow() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 713l, true);
		sub.lowerKey(49l);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetHigherKeyTooHigh() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 713l, true);
		sub.higherKey(900l);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFloorKeyTooLow() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 713l, true);
		sub.floorKey(49l);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testCeilingHigherKeyTooHigh() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 713l, true);
		sub.ceilingKey(900l);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetKeyTooLow() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 713l, true);
		sub.get(49l);
	}

	@Test(expected = IllegalArgumentException.class)
	public void testGetKeyTooHigh() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 713l, true);
		sub.get(900l);
	}

	@Test
	public void testContainsKey() {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 151l, true);
		assertTrue(sub.containsKey(57l));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testContainsKeyOutOfBoundsHigh() {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 151l, true);
		assertTrue(sub.containsKey(200l));
	}

	@Test(expected = IllegalArgumentException.class)
	public void testContainsKeyOutOfBoundsLow() {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 151l, true);
		assertTrue(sub.containsKey(49l));
	}

	@Test
	public void testSubMapOfSubMapInclusive() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 500l, true);
		ConcurrentNavigableMap<Long, Long> subSub = sub.subMap(100l, true, 200l, true);
		assertEquals(100l, (long)subSub.firstKey());
		assertEquals(200l, (long)subSub.lastKey());
	}

	@Test
	public void testSubMapOfSubMapNonInclusive() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 500l, true);
		ConcurrentNavigableMap<Long, Long> subSub = sub.subMap(100l, false, 200l, false);
		assertEquals(101l, (long)subSub.firstKey());
		assertEquals(199l, (long) subSub.lastKey());
	}

	@Test
	public void testSubMapOfSubMap() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 500l, true);
		ConcurrentNavigableMap<Long, Long> subSub = sub.subMap(100l, 200l);
		assertEquals(100l, (long)subSub.firstKey());
		assertEquals(199l, (long) subSub.lastKey());
	}

	@Test
	public void testHeadMap() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 500l, true);
		ConcurrentNavigableMap<Long, Long> subSub = sub.headMap(200l);
		assertEquals(50l, (long)subSub.firstKey());
		assertEquals(199l, (long) subSub.lastKey());
	}

	@Test
	public void testTailMap() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 500l, true);
		ConcurrentNavigableMap<Long, Long> subSub = sub.tailMap(200l);
		assertEquals(200l, (long)subSub.firstKey());
		assertEquals(500l, (long) subSub.lastKey());
	}

	@Test
	public void testPut() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 500l, true);
		sub.put(55l, 12345l);

		assertEquals(12345l, (long)sub.get(55l));
	}

	@Test
	public void testPutIfAbsent() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 1000l, true);
		sub.putIfAbsent(1000l, 12345l);

		assertEquals(12345l, (long)sub.get(1000l));
	}

	@Test
	public void testReplace() throws Exception {
		ConcurrentNavigableMap<Long, Long> sub = tree.subMap(50l, true, 1000l, true);
		sub.replace(500l, 12345l);

		assertEquals(12345l, (long)sub.get(500l));
	}

	private BTreeMap<Long, Long> addManyBulkSingleThread(Store store, int size, int maxNodes, Volume volume) throws Exception {

		BTreeMap<Long, Long> tree = new BTreeMap<>(store, comp, keySerializer, keySerializer, 100, false);

		try {

			for (int i = 0; i < size; i++) {
				BasicDBObject object = new BasicDBObject();
				object.put("_id", (long) i);
				object.put("firstname", "Ian");
				object.put("middlename", "Craig");
				object.put("surname", "Michell");
				try {
					long record = store.put(object, valueSerializer);
					tree.put((long) i, record);
				} catch (Throwable ex) {
					System.out.println("Failed at record: " + i + " next record offset: " + ((double)store.getHeader().getPosition() / 1024 / 1024 / 1024) + "GB volume size: " + ((double)volume.getLength() / 1024 / 1024 / 1024) + "GB");
					throw ex;
				}
			}

			// commit
			store.commit();

			// Validate that every element is in the datastore
			for (int i = 0; i < size; i++) {
				try {
					long record = tree.get((long)i);
					DBObject get = store.get(record, valueSerializer);
					assertNotNull("null get: i (" + i + ")", get);
					assertEquals((long) i, get.get("_id"));
				} catch (Throwable ex) {
					System.out.println("Failed at record: " + i);
					throw ex;
				}
			}
		} finally {
			store.close();
		}
		return tree;
	}
}
