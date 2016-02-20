package com.pungwe.db.types.btree;

import com.pungwe.db.io.serializers.DBDocumentSerializer;
import com.pungwe.db.io.serializers.LZ4Serializer;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.serializers.Serializers;
import com.pungwe.db.io.store.AppendOnlyStore;
import com.pungwe.db.io.store.DirectStore;
import com.pungwe.db.io.store.InstanceCachingStore;
import com.pungwe.db.io.store.Store;
import com.pungwe.db.io.volume.MemoryVolume;
import com.pungwe.db.io.volume.Volume;
import com.pungwe.db.types.BasicDBDocument;
import com.pungwe.db.types.DBDocument;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.concurrent.*;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;
import static org.junit.Assert.*;

/**
 * Created by ian on 15/10/2014.
 */
public class BTreeMapTest {

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
	private static Serializer<DBDocument> valueSerializer = new LZ4Serializer<>(new DBDocumentSerializer());

	@Test
	public void testAddKeyAndGet() throws Exception {
		BasicDBDocument object = new BasicDBDocument();
		object.put("_id", 1l);
		object.put("key", "value");
		Volume volume = new MemoryVolume(false);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		BTreeMap<Long, DBDocument> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);//new BTreeMapMap<>(store, comp, keySerializer, valueSerializer, true, 10, true);
		tree.put(1l, object);

		DBDocument get = tree.get(1l);
		assertEquals(object.get("_id"), get.get("_id"));
	}

	@Test
	public void testAddKeyAndUpdate() throws Exception {
		BasicDBDocument object = new BasicDBDocument();
		object.put("_id", 1l);
		object.put("key", "original");

		Volume volume = new MemoryVolume(false);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		BTreeMap<Long, DBDocument> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);//new BTreeMapMap<>(store, comp, keySerializer, valueSerializer, true, 10, true);
		tree.put(1l, object);

		DBDocument get = tree.get(1l);
		assertEquals(object.get("_id"), get.get("_id"));

		get.put("key", "new");
		tree.put(1l, get);

		get = tree.get(1l);
		assertEquals(object.get("_id"), get.get("_id"));
		assertEquals("new", get.get("key"));
	}

	@Test
	public void testAddMultipleKeysAndGet() throws Exception {
		Volume volume = new MemoryVolume(false);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		BTreeMap<Long, DBDocument> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 10; i++) {
			BasicDBDocument object = new BasicDBDocument();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		DBDocument get = tree.get(3l);
		assertNotNull(get);
		assertEquals(3l, get.get("_id"));
	}

	@Test
	public void testAddMultipleKeysAndIterate() throws Exception {
		Volume volume = new MemoryVolume(false);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		BTreeMap<Long, DBDocument> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 10000; i++) {
			BasicDBDocument object = new BasicDBDocument();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		Iterator<Map.Entry<Long, DBDocument>> it = new BTreeNodeIterator<Long, DBDocument>(tree);
		int i = 0;
		while (it.hasNext()) {
			Map.Entry<Long, DBDocument> e = it.next();
			assert e.getKey() == (long) i : "Key does not match: " + i + " : " + e.getKey();
			DBDocument get = e.getValue();
			assertEquals(get.get("_id"), (long) i);
			assertEquals(get.get("key"), "value");
			i++;
		}
		assertEquals(10000, i);
	}

	@Test
	public void testAddMultipleKeysAndIterateRangeInclusive() throws Exception {
		Volume volume = new MemoryVolume(false);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		BTreeMap<Long, DBDocument> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 10000; i++) {
			BasicDBDocument object = new BasicDBDocument();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		Iterator<Map.Entry<Long, DBDocument>> it = new BTreeNodeIterator<Long, DBDocument>(tree, 5123l, true, 6543l, true);
		int i = 5123;
		while (it.hasNext()) {
			Map.Entry<Long, DBDocument> e = it.next();
			assert e.getKey() == (long) i : "Key does not match: " + i + " : " + e.getKey();
			DBDocument get = e.getValue();
			assertEquals(get.get("_id"), (long) i);
			assertEquals(get.get("key"), "value");
			i++;
		}
		assertEquals(6544l, i);
	}

	@Test
	public void testAddMultipleKeysAndIterateBetween() throws Exception {
		Volume volume = new MemoryVolume(false);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		BTreeMap<Long, DBDocument> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 10000; i++) {
			BasicDBDocument object = new BasicDBDocument();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		Iterator<Map.Entry<Long, DBDocument>> it = new BTreeNodeIterator<Long, DBDocument>(tree, 5123l, false, 6543l, false);
		int i = 5124;
		while (it.hasNext()) {
			Map.Entry<Long, DBDocument> e = it.next();
			assert e.getKey() == (long) i : "Key does not match: " + i + " : " + e.getKey();
			DBDocument get = e.getValue();
			assertEquals((long) i, get.get("_id"));
			assertEquals("value", get.get("key"));
			i++;
		}
		assertEquals(6543, i);
	}

	@Test
	public void testAddMultipleKeysAndIterateDescending() throws Exception {
		Volume volume = new MemoryVolume(false);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		BTreeMap<Long, DBDocument> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 10000; i++) {
			BasicDBDocument object = new BasicDBDocument();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		Iterator<Map.Entry<Long, DBDocument>> it = new DescendingBTreeNodeIterator<Long, DBDocument>(tree);
		int i = 9999;
		int count = 0;
		while (it.hasNext()) {
			try {
				Map.Entry<Long, DBDocument> e = it.next();
				assert e.getKey() == (long) i : "Key does not match: " + i + " : " + e.getKey();
				DBDocument get = e.getValue();
				assertEquals(get.get("_id"), (long) i);
				assertEquals(get.get("key"), "value");
				i--;
				count++;
			} catch (Throwable t) {
				System.out.println("Failed at record: " + i);
				throw t;
			}
		}
		assertEquals(10000, count);
	}

	@Test
	public void testAddMultipleKeysAndIterateDescendingRangeInclusive() throws Exception {
		Volume volume = new MemoryVolume(false);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		BTreeMap<Long, DBDocument> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 10000; i++) {
			BasicDBDocument object = new BasicDBDocument();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		Iterator<Map.Entry<Long, DBDocument>> it = new DescendingBTreeNodeIterator<Long, DBDocument>(tree, 6543l, true, 5123l, true);
		int i = 6543;
		int count = 5123;
		while (it.hasNext()) {
			Map.Entry<Long, DBDocument> e = it.next();
			assert e.getKey() == (long) i : "Key does not match: " + i + " : " + e.getKey();
			DBDocument get = e.getValue();
			assertEquals(get.get("_id"), (long) i);
			assertEquals(get.get("key"), "value");
			i--;
			count++;
		}
		assertEquals(6544l, count);
	}

	@Test
	public void testAddMultipleKeysAndIterateDescendingBetween() throws Exception {
		Volume volume = new MemoryVolume(false);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		BTreeMap<Long, DBDocument> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 10000; i++) {
			BasicDBDocument object = new BasicDBDocument();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		Iterator<Map.Entry<Long, DBDocument>> it = new DescendingBTreeNodeIterator<Long, DBDocument>(tree, 6543l, false, 5123l, false);
		int i = 6542;
		int count = 5123;
		while (it.hasNext()) {
			Map.Entry<Long, DBDocument> e = it.next();
			assert e.getKey() == (long) i : "Key does not match: " + i + " : " + e.getKey();
			DBDocument get = e.getValue();
			assertEquals((long) i, get.get("_id"));
			assertEquals("value", get.get("key"));
			i--;
			count++;
		}
		assertEquals(6542, count);
	}

	@Test
	public void testGetLastEntry() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		AppendOnlyStore store = new AppendOnlyStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		BTreeMap<Long, DBDocument> tree = addManyBulkSingleThread(cacheStore, 1000, 100, volume);
		Map.Entry<Long, DBDocument> lastEntry = tree.lastEntry();
		assertNotNull(lastEntry);
		assertEquals(999l, lastEntry.getKey().longValue());
	}

	@Test
	public void testGetLastKey() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		AppendOnlyStore store = new AppendOnlyStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		BTreeMap<Long, DBDocument> tree = addManyBulkSingleThread(cacheStore, 1000, 100, volume);
		Long key = tree.lastKey();
		assertNotNull(key);
		assertEquals(999l, key.longValue());
	}

	@Test
	public void testFirstLastEntry() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		AppendOnlyStore store = new AppendOnlyStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		BTreeMap<Long, DBDocument> tree = addManyBulkSingleThread(cacheStore, 1000, 100, volume);
		Map.Entry<Long, DBDocument> firstEntry = tree.firstEntry();
		assertNotNull(firstEntry);
		assertEquals(0l, firstEntry.getKey().longValue());
	}

	@Test
	public void testFirstLastKey() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		AppendOnlyStore store = new AppendOnlyStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		BTreeMap<Long, DBDocument> tree = addManyBulkSingleThread(cacheStore, 1000, 100, volume);
		Long key = tree.firstKey();
		assertNotNull(key);
		assertEquals(0l, key.longValue());
	}

	@Test
	public void testLowerEntry() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		BTreeMap<Long, DBDocument> tree = addManyBulkSingleThread(cacheStore, 1000, 100, volume);
		assertEquals(51l, (long) tree.lowerKey(52l));
	}

	@Test
	public void testFloorEntry() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		BTreeMap<Long, DBDocument> tree = addManyBulkSingleThread(cacheStore, 1000, 100, volume);
		assertEquals(0l, (long) tree.floorKey(0l));
		assertEquals(51l, (long) tree.floorKey(52l));
	}

	@Test
	public void testHigherEntry() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		BTreeMap<Long, DBDocument> tree = addManyBulkSingleThread(cacheStore, 1000, 100, volume);
		assertEquals(52l, (long) tree.higherKey(51l));
	}

	@Test
	public void testCeilingEntry() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		BTreeMap<Long, DBDocument> tree = addManyBulkSingleThread(cacheStore, 1000, 100, volume);
		assertEquals(999l, (long) tree.ceilingKey(999l));
		assertEquals(713l, (long) tree.ceilingKey(712l));
	}

	@Test
	public void testEntryIterator() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		BTreeMap<Long, DBDocument> tree = addManyBulkSingleThread(cacheStore, 1000, 100, volume);

		Iterator<Map.Entry<Long, DBDocument>> it = tree.entryIterator();
		int i = 0;
		while (it.hasNext()) {
			Map.Entry<Long, DBDocument> e = it.next();
			assertNotNull(e);
			assertEquals((long)i++, (long)e.getKey());
		}
	}

	@Test
	public void testHeadMap() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		BTreeMap<Long, DBDocument> tree = addManyBulkSingleThread(cacheStore, 1000, 100, volume);
		ConcurrentNavigableMap<Long, DBDocument> sub = tree.headMap(200l);
		Assert.assertEquals(0l, (long) sub.firstKey());
		Assert.assertEquals(199l, (long) sub.lastKey());
	}

	@Test
	public void testTailMap() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		BTreeMap<Long, DBDocument> tree = addManyBulkSingleThread(cacheStore, 1000, 100, volume);
		ConcurrentNavigableMap<Long, DBDocument> sub = tree.tailMap(900l);
		Assert.assertEquals(901l, (long) sub.firstKey());
		Assert.assertEquals(999l, (long) sub.lastKey());
	}

	@Test
	public void testPutAll() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		BTreeMap<Long, Long> tree = new BTreeMap<>(store, comp, keySerializer, new Serializers.NUMBER(), 100, false);

		HashMap<Long, Long> objects = new HashMap<>(1000);
		for (int i = 0; i < 1000; i++) {
			BasicDBDocument object = new BasicDBDocument();
			object.put("_id", (long) i);
			object.put("firstname", "Ian");
			object.put("middlename", "Craig");
			object.put("surname", "Michell");
			long record = store.put(object, valueSerializer);
			objects.put((long)i, record);
		}

		tree.putAll(objects);

		for (int i = 0; i < 1000; i++) {
			try {
				Long record = tree.get((long) i);
				assertNotNull(record);
				DBDocument get = store.get(record, valueSerializer);
				assertNotNull("null get: i (" + i + ")", get);
				assertEquals((long) i, get.get("_id"));
			} catch (Throwable ex) {
				System.out.println("Failed at record: " + i);
				throw ex;
			}
		}
	}

	// FIXME: Check the split
	@Test
	public void testAddAndSplit() throws Exception {
		Volume volume = new MemoryVolume(false);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		BTreeMap<Long, DBDocument> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 20; i++) {
			BasicDBDocument object = new BasicDBDocument();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		for (long i = 0; i < 20; i++) {
			DBDocument get = tree.get(i);
			assertNotNull("Get must not be null: " + i, get);
			assertEquals(i, get.get("_id"));
		}
		assertTrue(volume.getLength() > 0);
	}

	@Test
	public void testContainsKey() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		BTreeMap<Long, Long> tree = new BTreeMap<>(store, comp, keySerializer, new Serializers.NUMBER(), 100, false);

		HashMap<Long, Long> objects = new HashMap<>(1000);
		for (int i = 0; i < 1000; i++) {
			BasicDBDocument object = new BasicDBDocument();
			object.put("_id", (long) i);
			object.put("firstname", "Ian");
			object.put("middlename", "Craig");
			object.put("surname", "Michell");
			long record = store.put(object, valueSerializer);
			objects.put((long)i, record);
		}

		tree.putAll(objects);

		assertTrue(tree.containsKey(57l));
	}

	@Test
	public void testContainsValue() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		BTreeMap<Long, Long> tree = new BTreeMap<>(store, comp, keySerializer, new Serializers.NUMBER(), 100, false);

		HashMap<Long, Long> objects = new HashMap<>(1000);
		for (int i = 0; i < 1000; i++) {
			BasicDBDocument object = new BasicDBDocument();
			object.put("_id", (long) i);
			object.put("firstname", "Ian");
			object.put("middlename", "Craig");
			object.put("surname", "Michell");
			long record = store.put(object, valueSerializer);
			objects.put((long)i, record);
		}

		tree.putAll(objects);

		long o = tree.get(55l);
		assertNotNull(o);
		assertTrue(tree.containsValue(o));
	}

	@Test
	public void testIsEmpty() throws Exception {
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		BTreeMap<Long, Long> tree = new BTreeMap<>(store, comp, keySerializer, new Serializers.NUMBER(), 100, false);
		assert tree.isEmpty() : "tree not empty";
		tree.put(1l, 1l);
		assert !tree.isEmpty() : "tree is empty";
	}

	@Test
	public void testAddManyMemoryHeap() throws Exception {
//		System.out.println("Memory Heap");
		Volume volume = new MemoryVolume(false, 30);
		final Volume recVolume = new MemoryVolume(false, 20);
		DirectStore store = new DirectStore(volume, recVolume);
		InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
		addManyBulkSingleThread(cacheStore, 10000, 100, volume);
	}

	private BTreeMap<Long, DBDocument> addManyBulkSingleThread(Store store, int size, int maxNodes, Volume volume) throws Exception {

		BTreeMap<Long, DBDocument> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 100, true);

		try {
			for (int i = 0; i < size; i++) {
				BasicDBDocument object = new BasicDBDocument();
				object.put("_id", (long) i);
				object.put("firstname", "Ian");
				object.put("middlename", "Craig");
				object.put("surname", "Michell");

				try {
					tree.put((long) i, object);
				} catch (Throwable ex) {
					System.out.println("Failed at record: " + i + " next record offset: " + ((double) store.getHeader().getPosition() / 1024 / 1024 / 1024) + "GB volume size: " + ((double) volume.getLength() / 1024 / 1024 / 1024) + "GB");
					throw ex;
				}
			}

			// commit
			store.commit();

			// Validate that every element is in the datastore
			for (int i = 0; i < size; i++) {
				try {
					DBDocument get = tree.get((long) i);
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

	@Test
	public void testAddManyMultiThreaded() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(8);

		try {
			final Volume volume = new MemoryVolume(false, 30);
			final Volume recVolume = new MemoryVolume(false, 20);
			AppendOnlyStore store = new AppendOnlyStore(volume, recVolume);
			InstanceCachingStore cacheStore = new InstanceCachingStore(store, 1000);
			final BTreeMap<Long, DBDocument> tree = new BTreeMap<>(cacheStore, comp, keySerializer, valueSerializer, 100, true);
			Collection<Callable<Long>> threads = new LinkedList<>();
			for (int i = 0; i < 1000; i++) {
				final long key = (long) i;
				threads.add(new Callable() {
					@Override
					public Object call() {
						try {
							BasicDBDocument object = new BasicDBDocument();
							object.put("_id", key);
							object.put("firstname", "Ian");
							object.put("middlename", "Craig");
							object.put("surname", "Michell");

							tree.put(key, object);

						} catch (Exception ex) {
							System.out.println("Failed at record: " + key);
							ex.printStackTrace();
							return null;
						}
						return key;
					}
				});
			}

			executor.invokeAll(threads, 60, TimeUnit.SECONDS);
			// Validate that every element is in the datastore
			for (int i = 0; i < 1000; i++) {
				try {
					DBDocument get = tree.get((long) i);
					assertNotNull("null get: i (" + i + ")", get);
					assertEquals((long) i, get.get("_id"));
				} catch (Throwable ex) {
					System.out.println("Failed at record: " + i);
					throw ex;
				}
			}

		} finally {
			executor.shutdown();
		}
	}

}
