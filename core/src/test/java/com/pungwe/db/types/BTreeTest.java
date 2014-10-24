package com.pungwe.db.types;

import com.pungwe.db.io.RandomAccessFileStore;
import com.pungwe.db.io.TreeMapHeapStore;
import com.pungwe.db.io.serializers.DBObjectSerializer;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.serializers.Serializers;
import org.junit.Test;

import java.io.File;
import java.util.Comparator;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

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

	@Test
	public void testAddKeyNoGet() throws Exception {
		BasicDBObject object = new BasicDBObject();
		object.put("_id", 1);
		object.put("key", "value");
		TreeMapHeapStore store = new TreeMapHeapStore();
		BTree<Long, DBObject> tree = new BTree<>(store, comp, null, null, true, 10, true);
		tree.add(1l, object);

		assertEquals(2, store.getData().size());
		assertEquals(object, store.get(0, null));
	}

	@Test
	public void testAddKeyAndGet() throws Exception {
		BasicDBObject object = new BasicDBObject();
		object.put("_id", 1);
		object.put("key", "value");
		TreeMapHeapStore store = new TreeMapHeapStore();
		BTree<Long, DBObject> tree = new BTree<>(store, comp, null, null, true, 10, true);
		tree.add(1l, object);

		DBObject get = tree.get(0l);
		assertEquals(object, get);
	}

	@Test
	public void testAddMultipleKeysAndGet() throws Exception {
		TreeMapHeapStore store = new TreeMapHeapStore();
		BTree<Long, DBObject> tree = new BTree<Long, DBObject>(store, comp, null, null, true, 10, true);

		for (int i = 0; i < 10; i++) {
			BasicDBObject object = new BasicDBObject();
			object.put("_id", (long)i);
			object.put("key", "value");
			tree.add((long)i, object);
		}

		DBObject get = tree.get(3l);
		assertEquals(3l, get.get("_id"));
	}

	@Test
	public void testAddAndSplit() throws Exception {
		TreeMapHeapStore store = new TreeMapHeapStore();
		BTree<Long, DBObject> tree = new BTree<>(store, comp, null, null, true, 10, true);

		for (int i = 0; i < 11; i++) {
			BasicDBObject object = new BasicDBObject();
			object.put("_id", i);
			object.put("key", "value");
			tree.add((long)i, object);
		}

		DBObject get = tree.get(3l);
		assertEquals(3, get.get("_id"));
		get = tree.get(10l);
		assertEquals(10, get.get("_id"));
		assertEquals(14, store.getData().size());
	}

	@Test
	public void testAddKeyToFileStore() throws Exception {
		// Create tmp file
		File file = File.createTempFile(UUID.randomUUID().toString(), "");
		try {
			RandomAccessFileStore store = new RandomAccessFileStore(file);
			Serializer<Long> keySerializer = new Serializers.NUMBER();
			Serializer<DBObject> valueSerializer = new DBObjectSerializer();
			BTree<Long, DBObject> tree = new BTree<Long, DBObject>(store, comp, new Serializers.NUMBER(), new DBObjectSerializer(), true, 10, true);
			BasicDBObject object = new BasicDBObject();
			object.put("_id", 1l);
			object.put("key", "value");

			tree.add(1l, object);

			DBObject get = tree.get(1l);
			assertEquals(object, get);
		} finally {
			file.delete();
		}
	}
}
