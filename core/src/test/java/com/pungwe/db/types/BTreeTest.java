package com.pungwe.db.types;

import com.pungwe.db.io.store.AppendOnlyStore;
import com.pungwe.db.io.store.DirectStore;
import com.pungwe.db.io.store.MemoryStore;
import com.pungwe.db.io.serializers.DBObjectSerializer;
import com.pungwe.db.io.serializers.LZ4Serializer;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.serializers.Serializers;
import com.pungwe.db.io.volume.MappedFileVolume;
import com.pungwe.db.io.volume.RandomAccessFileVolume;
import com.pungwe.db.io.volume.Volume;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by ian on 15/10/2014.
 */
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
	public void testAddKeyNoGet() throws Exception {
		BasicDBObject object = new BasicDBObject();
		object.put("_id", 1l);
		object.put("key", "value");
		TreeMapHeapStore store = new TreeMapHeapStore();
		BTree<Long, DBObject> tree = new BTree<>(store, comp, keySerializer, valueSerializer, true, 10, true);
		tree.add(1l, object);

		assertEquals(2, store.getData().size());
		assertEquals(object.get("_id"), store.get(1l, valueSerializer).get("_id"));
	}

	@Test
	public void testAddKeyAndGet() throws Exception {
		BasicDBObject object = new BasicDBObject();
		object.put("_id", 1l);
		object.put("key", "value");
		TreeMapHeapStore store = new TreeMapHeapStore();
		BTree<Long, DBObject> tree = new BTree<>(store, comp, keySerializer, valueSerializer, true, 10, true);
		tree.add(1l, object);

		DBObject get = tree.get(1l);
		assertEquals(object.get("_id"), get.get("_id"));
	}

	@Test
	public void testAddMultipleKeysAndGet() throws Exception {
		TreeMapHeapStore store = new TreeMapHeapStore();
		BTree<Long, DBObject> tree = new BTree<Long, DBObject>(store, comp, keySerializer, valueSerializer, true, 10, true);

		for (int i = 0; i < 10; i++) {
			BasicDBObject object = new BasicDBObject();
			object.put("_id", (long)i);
			object.put("key", "value");
			tree.add((long)i, object);
		}

		DBObject get = tree.get(3l);
		assertNotNull(get);
		assertEquals(3l, get.get("_id"));
	}

	@Test
	public void testAddAndSplit() throws Exception {
		TreeMapHeapStore store = new TreeMapHeapStore();
		BTree<Long, DBObject> tree = new BTree<>(store, comp, keySerializer, valueSerializer, true, 10, true);

		for (int i = 0; i < 20; i++) {
			BasicDBObject object = new BasicDBObject();
			object.put("_id", (long)i);
			object.put("key", "value");
			tree.add((long)i, object);
		}

		for (long i = 0; i < 20; i++) {
			DBObject get = tree.get(i);
			assertNotNull(get);
			assertEquals(i, get.get("_id"));
		}
		assertEquals(24l, store.getData().size());
	}

	@Test
	public void addManyBulkSingleThread() throws Exception {

		File file = File.createTempFile("tmp", "db");
		file.deleteOnExit();
		List<Pointer> pointers = new ArrayList<Pointer>();
		//MemoryStore store = new MemoryStore(/*256 * 1024 * 1024*/ Integer.MAX_VALUE); // 1GB

		//Volume volume = new RandomAccessFileVolume(file, false);
		Volume volume = new MappedFileVolume(file, false, 256 * 1024 * 1024);
		//AppendOnlyStore store = new AppendOnlyStore(volume);
		DirectStore store = new DirectStore(volume);
		Serializer<Long> keySerializer = new Serializers.NUMBER();
		Serializer<DBObject> valueSerializer = new LZ4Serializer<>(new DBObjectSerializer());
		BTree<Long, Pointer> tree = new BTree<Long, Pointer>(store, comp, keySerializer, null, true, 100, false);

		try {
			long start = System.nanoTime();
			for (int i = 0; i < 100000; i++) {
				BasicDBObject object = new BasicDBObject();
				object.put("_id", (long) i);
				object.put("firstname", "Ian");
				object.put("middlename", "Craig");
				object.put("surname", "Michell");
				try {
					long p = store.put(object, valueSerializer);
					pointers.add(new Pointer(p));
				} catch (AssertionError ex) {
					System.out.println("Failed at record: " + i);
					throw ex;
				} catch (Exception ex) {
					System.out.println("Failed at record: " + i);
					throw ex;
				}
			}
			long end = System.nanoTime();

			System.out.println("It took: " + ((end - start) / 1000000000d) + " seconds to bulk write 100 " + volume.getLength());

			start = System.nanoTime();
			for (int i = 0; i < 100000; i++) {
				Pointer p = pointers.get(i);
				tree.add((long)i, p);
			}

			// commit
			store.commit();

			end = System.nanoTime();

			System.out.println("It took: " + ((end - start) / 1000000000d) + " seconds to index 100 "  + volume.getLength());

			start = System.nanoTime();
			// Validate that every element is in the datastore
			for (int i = 0; i < 100000; i++) {
				try {
					Pointer p = tree.get((long) i);
					DBObject get = store.get(p.getPointer(), valueSerializer);
					assertNotNull("null get: i (" + i + ")", get);
					assertEquals((long) i, get.get("_id"));
				} catch (Throwable ex) {
					System.out.println("Failed at record: " + i);
					throw ex;
				}
			}
			end = System.nanoTime();
			System.out.println("It took: " + ((end - start) / 1000000000d) + " seconds to bulk read 100");

		} finally {
			store.close();
		}
	}

}
