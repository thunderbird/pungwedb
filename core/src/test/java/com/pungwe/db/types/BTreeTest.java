package com.pungwe.db.types;

import com.pungwe.db.io.serializers.DBObjectSerializer;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.serializers.Serializers;
import org.junit.Test;

import java.util.Comparator;

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
		assertEquals(object.get("_id"), store.get(0, valueSerializer).get("_id"));
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
	public void testAddALotOfRecordsSingleThread() throws Exception {
		TreeMapHeapStore store = new TreeMapHeapStore();
		try {
			Serializer<Long> keySerializer = new Serializers.NUMBER();
			Serializer<DBObject> valueSerializer = new DBObjectSerializer();
			BTree<Long, DBObject> tree = new BTree<Long, DBObject>(store, comp, keySerializer, valueSerializer, true, 1000, true);


			for (int i = 0; i < 50000; i++) {
				BasicDBObject object = new BasicDBObject();
				object.put("_id", (long) i);
				object.put("key", "value");
				tree.add((long) i, object);
			}

			// Validate that every element is in the datastore
			for (int i = 0; i < 50000; i++) {
				DBObject get = tree.get((long)i);
				assertNotNull("null get: i (" + i + ")", get);
				assertEquals((long)i, get.get("_id"));
			}
			// Validate that every element is in the datastore
			DBObject get = tree.get(2991l);
			assertNotNull("null get: i (" + 2991l + ")", get);
			assertEquals(2991l, get.get("_id"));
		} finally {
			store.close();
		}
	}
}
