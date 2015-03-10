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

import static org.junit.Assert.assertEquals;
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
	private BTreeMap<Long, DBObject> tree;

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
	public void testGetSubMap() throws Exception {
		ConcurrentNavigableMap<Long, DBObject> sub = tree.subMap(50l, false, 100l, false);
		assertEquals(51l, (long)sub.firstKey());
		assertEquals(99l, (long)sub.lastKey());
	}

	private BTreeMap<Long, DBObject> addManyBulkSingleThread(Store store, int size, int maxNodes, Volume volume) throws Exception {

		BTreeMap<Long, DBObject> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 100, true);

		try {

			for (int i = 0; i < size; i++) {
				BasicDBObject object = new BasicDBObject();
				object.put("_id", (long) i);
				object.put("firstname", "Ian");
				object.put("middlename", "Craig");
				object.put("surname", "Michell");

				try {
					tree.put((long) i, object);
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
					DBObject get = tree.get((long)i);
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
