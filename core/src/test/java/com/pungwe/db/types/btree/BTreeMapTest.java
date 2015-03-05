package com.pungwe.db.types.btree;

import com.pungwe.db.io.serializers.DBObjectSerializer;
import com.pungwe.db.io.serializers.LZ4Serializer;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.serializers.Serializers;
import com.pungwe.db.io.store.AppendOnlyStore;
import com.pungwe.db.io.store.DirectStore;
import com.pungwe.db.io.store.Store;
import com.pungwe.db.io.volume.MappedFileVolume;
import com.pungwe.db.io.volume.MemoryVolume;
import com.pungwe.db.io.volume.Volume;
import com.pungwe.db.types.BasicDBObject;
import com.pungwe.db.types.DBObject;
import com.pungwe.db.types.btree.BTreeMap;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

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
	private static Serializer<DBObject> valueSerializer = new LZ4Serializer<>(new DBObjectSerializer());

	@Test
	public void testAddKeyAndGet() throws Exception {
		BasicDBObject object = new BasicDBObject();
		object.put("_id", 1l);
		object.put("key", "value");
		Volume volume = new MemoryVolume(false);
		DirectStore store = new DirectStore(volume);
		BTreeMap<Long, DBObject> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);//new BTreeMapMap<>(store, comp, keySerializer, valueSerializer, true, 10, true);
		tree.put(1l, object);

		DBObject get = tree.get(1l);
		assertEquals(object.get("_id"), get.get("_id"));
	}

	@Test
	public void testAddKeyAndUpdate() throws Exception {
		BasicDBObject object = new BasicDBObject();
		object.put("_id", 1l);
		object.put("key", "original");

		Volume volume = new MemoryVolume(false);
		DirectStore store = new DirectStore(volume);
		BTreeMap<Long, DBObject> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);//new BTreeMapMap<>(store, comp, keySerializer, valueSerializer, true, 10, true);
		tree.put(1l, object);

		DBObject get = tree.get(1l);
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
		DirectStore store = new DirectStore(volume);
		BTreeMap<Long, DBObject> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 10; i++) {
			BasicDBObject object = new BasicDBObject();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		DBObject get = tree.get(3l);
		assertNotNull(get);
		assertEquals(3l, get.get("_id"));
	}

	@Test
	public void testAddMultipleKeysAndIterate() throws Exception {
		Volume volume = new MemoryVolume(false);
		DirectStore store = new DirectStore(volume);
		BTreeMap<Long, DBObject> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 10000; i++) {
			BasicDBObject object = new BasicDBObject();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		Iterator<Map.Entry<Long, DBObject>> it = new BTreeNodeIterator<Long, DBObject>(tree);
		int i = 0;
		while (it.hasNext()) {
			Map.Entry<Long, DBObject> e = it.next();
			assert e.getKey() == (long)i : "Key does not match: " + i + " : " +  e.getKey();
			DBObject get = e.getValue();
			assertEquals(get.get("_id"), (long)i);
			assertEquals(get.get("key"), "value");
			i++;
		}
		assertEquals(10000, i);
	}

	@Test
	public void testAddMultipleKeysAndIterateRangeInclusive() throws Exception {
		Volume volume = new MemoryVolume(false);
		DirectStore store = new DirectStore(volume);
		BTreeMap<Long, DBObject> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 10000; i++) {
			BasicDBObject object = new BasicDBObject();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		Iterator<Map.Entry<Long, DBObject>> it = new BTreeNodeIterator<Long, DBObject>(tree, 5123l, true, 6543l, true);
		int i = 5123;
		while (it.hasNext()) {
			Map.Entry<Long, DBObject> e = it.next();
			assert e.getKey() == (long)i : "Key does not match: " + i + " : " +  e.getKey();
			DBObject get = e.getValue();
			assertEquals(get.get("_id"), (long)i);
			assertEquals(get.get("key"), "value");
			i++;
		}
		assertEquals(6544l, i);
	}

	@Test
	public void testAddMultipleKeysAndIterateBetween() throws Exception {
		Volume volume = new MemoryVolume(false);
		DirectStore store = new DirectStore(volume);
		BTreeMap<Long, DBObject> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 10000; i++) {
			BasicDBObject object = new BasicDBObject();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		Iterator<Map.Entry<Long, DBObject>> it = new BTreeNodeIterator<Long, DBObject>(tree, 5123l, false, 6543l, false);
		int i = 5124;
		while (it.hasNext()) {
			Map.Entry<Long, DBObject> e = it.next();
			assert e.getKey() == (long)i : "Key does not match: " + i + " : " +  e.getKey();
			DBObject get = e.getValue();
			assertEquals((long)i, get.get("_id"));
			assertEquals("value",get.get("key"));
			i++;
		}
		assertEquals(6543, i);
	}

	@Test
	public void testAddMultipleKeysAndIterateDescending() throws Exception {
		Volume volume = new MemoryVolume(false);
		DirectStore store = new DirectStore(volume);
		BTreeMap<Long, DBObject> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 10000; i++) {
			BasicDBObject object = new BasicDBObject();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		Iterator<Map.Entry<Long, DBObject>> it = new DescendingBTreeNodeIterator<Long, DBObject>(tree);
		int i = 9999;
		int count = 0;
		while (it.hasNext()) {
			try {
				Map.Entry<Long, DBObject> e = it.next();
				assert e.getKey() == (long) i : "Key does not match: " + i + " : " + e.getKey();
				DBObject get = e.getValue();
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
		DirectStore store = new DirectStore(volume);
		BTreeMap<Long, DBObject> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 10000; i++) {
			BasicDBObject object = new BasicDBObject();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		Iterator<Map.Entry<Long, DBObject>> it = new DescendingBTreeNodeIterator<Long, DBObject>(tree, 5123l, true, 6543l, true);
		int i = 6543;
		int count = 5123;
		while (it.hasNext()) {
			Map.Entry<Long, DBObject> e = it.next();
			assert e.getKey() == (long)i : "Key does not match: " + i + " : " +  e.getKey();
			DBObject get = e.getValue();
			assertEquals(get.get("_id"), (long)i);
			assertEquals(get.get("key"), "value");
			i--;
			count++;
		}
		assertEquals(6544l, count);
	}

	@Test
	public void testAddMultipleKeysAndIterateDescendingBetween() throws Exception {
		Volume volume = new MemoryVolume(false);
		DirectStore store = new DirectStore(volume);
		BTreeMap<Long, DBObject> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 10000; i++) {
			BasicDBObject object = new BasicDBObject();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		Iterator<Map.Entry<Long, DBObject>> it = new DescendingBTreeNodeIterator<Long, DBObject>(tree, 5123l, false, 6543l, false);
		int i = 6542;
		int count = 5123;
		while (it.hasNext()) {
			Map.Entry<Long, DBObject> e = it.next();
			assert e.getKey() == (long)i : "Key does not match: " + i + " : " +  e.getKey();
			DBObject get = e.getValue();
			assertEquals((long)i, get.get("_id"));
			assertEquals("value",get.get("key"));
			i--;
			count++;
		}
		assertEquals(6542, count);
	}

	// FIXME: Check the split
	@Test
	public void testAddAndSplit() throws Exception {
		Volume volume = new MemoryVolume(false);
		DirectStore store = new DirectStore(volume);
		BTreeMap<Long, DBObject> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 20; i++) {
			BasicDBObject object = new BasicDBObject();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.put((long) i, object);
		}

		for (long i = 0; i < 20; i++) {
			DBObject get = tree.get(i);
			assertNotNull("Get must not be null: " + i, get);
			assertEquals(i, get.get("_id"));
		}
		assertTrue(volume.getLength() > 0);
	}

	@Test
	public void testAddManyMemoryHeap() throws Exception {
		System.out.println("Memory Heap");
		Volume volume = new MemoryVolume(false, 30);
		DirectStore store = new DirectStore(volume);
		addManyBulkSingleThread(store, 100000, 100, volume);
	}

	@Test
	public void testAddManyMemoryDirect() throws Exception {
		System.out.println("Memory Direct");
		Volume volume = new MemoryVolume(true, 30);
		DirectStore store = new DirectStore(volume);
		addManyBulkSingleThread(store, 100000, 100, volume);
	}

	@Test
	public void testAddManyAppendOnly() throws Exception {
		System.out.println("Append Only");
		Volume volume = new MemoryVolume(false, 30);
		AppendOnlyStore store = new AppendOnlyStore(volume);
		addManyBulkSingleThread(store, 100000, 100, volume);
	}

	@Test
	public void testAddManyMapped() throws Exception {
		System.out.println("Memory Mapped");
		File file = File.createTempFile("tmp", "db");
		file.deleteOnExit();
		Volume volume = new MappedFileVolume(file, false, 30);
		DirectStore store = new DirectStore(volume);
		addManyBulkSingleThread(store, 100000, 1000, volume);
	}

	private void addManyBulkSingleThread(Store store, int size, int maxNodes, Volume volume) throws Exception {

		BTreeMap<Long, DBObject> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 100, true);

		try {
			long start = System.nanoTime();
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

			long end = System.nanoTime();

			System.out.println("It took: " + ((end - start) / 1000000000d) + " seconds to index " + size + ": " + volume.getLength() / 1024 / 1024 + "MB");

			start = System.nanoTime();
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
			end = System.nanoTime();
			System.out.println("It took: " + ((end - start) / 1000000000d) + " seconds to bulk read " + size + ": " + volume.getLength() / 1024 / 1024 + "MB");

		} finally {
			store.close();
		}
	}

	@Test
	@Ignore
	public void testAddManyMultiThreaded() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(100);

		try {
			System.out.println("Multi threaded");
			File file = File.createTempFile("tmp", "db");
			file.deleteOnExit();
			Volume volume = new MappedFileVolume(file, false, 30);
			final DirectStore store = new DirectStore(volume);
			final BTreeMap<Long, DBObject> tree = new BTreeMap<>(store, comp, keySerializer, valueSerializer, 100, true);
			Collection<Callable<Long>> threads = new LinkedList<>();
			for (int i = 0; i < 100000; i++) {
				final long key = (long) i;
				threads.add(new Callable() {
					@Override
					public Object call() {
						try {
							BasicDBObject object = new BasicDBObject();
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

			long start = System.nanoTime();

			executor.invokeAll(threads, 60, TimeUnit.SECONDS);

			long end = System.nanoTime();

			System.out.println("It took: " + ((end - start) / 1000000000d) + " seconds to save and index " + 100000 + ": " + volume.getLength() / 1024 / 1024 + "MB");

			start = System.nanoTime();
			// Validate that every element is in the datastore
			for (int i = 0; i < 100000; i++) {
				try {
					DBObject get = tree.get((long) i);
					assertNotNull("null get: i (" + i + ")", get);
					assertEquals((long) i, get.get("_id"));
				} catch (Throwable ex) {
					System.out.println("Failed at record: " + i);
					throw ex;
				}
			}
			end = System.nanoTime();
			System.out.println("It took: " + ((end - start) / 1000000000d) + " seconds to bulk read " + 100000 + ": " + volume.getLength() / 1024 / 1024 + "MB");
		} finally {
			executor.shutdown();
		}
	}

}
