package com.pungwe.db.types;

import com.pungwe.db.io.store.AppendOnlyStore;
import com.pungwe.db.io.store.DirectStore;
import com.pungwe.db.io.serializers.DBObjectSerializer;
import com.pungwe.db.io.serializers.LZ4Serializer;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.serializers.Serializers;
import com.pungwe.db.io.store.Store;
import com.pungwe.db.io.volume.*;
import org.junit.Test;

import java.io.File;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static org.junit.Assert.*;

/**
 * Created by ian on 15/10/2014.
 */
@org.junit.Ignore
public class BTreeTest {

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
	private static Serializer<DBObject> valueSerializer = new DBObjectSerializer();

	@Test
	public void testAddKeyAndGet() throws Exception {
		BasicDBObject object = new BasicDBObject();
		object.put("_id", 1l);
		object.put("key", "value");
		Volume volume = new MemoryVolume(false);
		DirectStore store = new DirectStore(volume);
		BTree<Long, DBObject> tree = new BTree<>(store, comp, keySerializer, valueSerializer, 10, true);//new BTreeMap<>(store, comp, keySerializer, valueSerializer, true, 10, true);
		tree.add(1l, object);

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
		BTree<Long, DBObject> tree = new BTree<>(store, comp, keySerializer, valueSerializer, 10, true);//new BTreeMap<>(store, comp, keySerializer, valueSerializer, true, 10, true);
		tree.add(1l, object);

		DBObject get = tree.get(1l);
		assertEquals(object.get("_id"), get.get("_id"));

		get.put("key", "new");
		tree.update(1l, get);

		get = tree.get(1l);
		assertEquals(object.get("_id"), get.get("_id"));
		assertEquals("new", get.get("key"));
	}

	@Test
	public void testAddMultipleKeysAndGet() throws Exception {
		Volume volume = new MemoryVolume(false);
		DirectStore store = new DirectStore(volume);
		BTree<Long, DBObject> tree = new BTree<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 10; i++) {
			BasicDBObject object = new BasicDBObject();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.add((long) i, object);
		}

		DBObject get = tree.get(3l);
		assertNotNull(get);
		assertEquals(3l, get.get("_id"));
	}

	// FIXME: Check the split
	@Test
	public void testAddAndSplit() throws Exception {
		Volume volume = new MemoryVolume(false);
		DirectStore store = new DirectStore(volume);
		BTree<Long, DBObject> tree = new BTree<>(store, comp, keySerializer, valueSerializer, 10, true);

		for (int i = 0; i < 20; i++) {
			BasicDBObject object = new BasicDBObject();
			object.put("_id", (long) i);
			object.put("key", "value");
			tree.add((long) i, object);
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
		addManyBulkSingleThread(store, 100000, volume);
	}

	@Test
	public void testAddManyMemoryDirect() throws Exception {
		System.out.println("Memory Direct");
		Volume volume = new MemoryVolume(true, 30);
		DirectStore store = new DirectStore(volume);
		addManyBulkSingleThread(store, 100000, volume);
	}

	@Test
	public void testAddManyAppendOnly() throws Exception {
		System.out.println("Append Only");
		Volume volume = new MemoryVolume(false, 30);
		AppendOnlyStore store = new AppendOnlyStore(volume);
		addManyBulkSingleThread(store, 100000, volume);
	}

	@Test
	public void testAddManyMapped() throws Exception {
		System.out.println("Memory Mapped");
		File file = File.createTempFile("tmp", "db");
		file.deleteOnExit();
		Volume volume = new MappedFileVolume(file, false, 30);
		DirectStore store = new DirectStore(volume);
		addManyBulkSingleThread(store, 100000, volume);
	}

	private void addManyBulkSingleThread(Store store, int size, Volume volume) throws Exception {

		BTree<Long, DBObject> tree = new BTree<>(store, comp, keySerializer, null, 100, false);

		try {
			long start = System.nanoTime();
			for (int i = 0; i < size; i++) {
				BasicDBObject object = new BasicDBObject();
				object.put("_id", (long) i);
				object.put("firstname", "Ian");
				object.put("middlename", "Craig");
				object.put("surname", "Michell");

				try {
					tree.add((long) i, object);
				} catch (Throwable ex) {
					System.out.println("Failed at record: " + i);
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
	public void testAddManyMultiThreaded() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(1000);

		try {
			System.out.println("Multi threaded");
			Volume volume = new MemoryVolume(false, 30);
			final DirectStore store = new DirectStore(volume);
			final BTree<Long, Pointer> tree = new BTree<>(store, comp, keySerializer, null, 100, false);
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

							long pointer = store.put(object, new DBObjectSerializer());
							tree.add(key, new Pointer(pointer));

						} catch (Exception ex) {
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
					Pointer p = tree.get((long) i);
					assertNotNull("Pointer should not be null", p);
					DBObject get = store.get(p.getPointer(), valueSerializer);
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
