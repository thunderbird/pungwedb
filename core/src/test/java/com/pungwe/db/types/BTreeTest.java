package com.pungwe.db.types;

import com.pungwe.db.io.TreeMapHeapStore;
import org.junit.Test;

import java.util.Comparator;

import static org.junit.Assert.assertEquals;

/**
 * Created by ian on 15/10/2014.
 */
public class BTreeTest {

	private static final Comparator<Integer> comp = new Comparator<Integer>() {

		@Override
		public int compare(Integer o1, Integer o2) {
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
		BTree<Integer, DBObject> tree = new BTree<>(store, comp, null, null, "db", "collection", "index", true, false, 10, true);
		tree.add(1, object);

		assertEquals(2, store.getData().size());
		assertEquals(object, store.get(0, null));
	}

	@Test
	public void testAddKeyAndGet() throws Exception {
		BasicDBObject object = new BasicDBObject();
		object.put("_id", 1);
		object.put("key", "value");
		TreeMapHeapStore store = new TreeMapHeapStore();
		BTree<Integer, DBObject> tree = new BTree<>(store, comp, null, null, "db", "collection", "index", true, false, 10, true);
		tree.add(1, object);

		DBObject get = tree.get(0);
		assertEquals(object, get);
	}

	@Test
	public void testAddMultipleKeysAndGet() throws Exception {
		TreeMapHeapStore store = new TreeMapHeapStore();
		BTree<Integer, DBObject> tree = new BTree<>(store, comp, null, null, "db", "collection", "index", true, false, 10, true);

		for (int i = 0; i < 10; i++) {
			BasicDBObject object = new BasicDBObject();
			object.put("_id", i);
			object.put("key", "value");
			tree.add(i, object);
		}

		DBObject get = tree.get(3);
		assertEquals(3, get.get("_id"));
	}
}
