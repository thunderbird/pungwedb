package com.pungwe.db.io.store;

import com.pungwe.db.io.serializers.DBObjectSerializer;
import com.pungwe.db.io.volume.MemoryVolume;
import com.pungwe.db.io.volume.Volume;
import com.pungwe.db.types.BasicDBObject;
import com.pungwe.db.types.DBObject;
import com.pungwe.db.types.Header;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by 917903 on 13/02/2015.
 */
public class DirectStoreTest {

	Volume volume;
	DirectStore store;

	@Before
	public void setupBuffer() throws Exception {
		volume = new MemoryVolume(false);
		store = new DirectStore(volume);
	}

	@Test
	public void testPut() throws Exception {
		BasicDBObject object = new BasicDBObject();
		object.put("_id", 1l);
		object.put("key", "value");
		long position = store.put(object, new DBObjectSerializer());
		assert position > 0 : "Position should be greater than -1";

	}

	@Test
	public void testGet() throws Exception {
		BasicDBObject object = new BasicDBObject();
		object.put("_id", 1l);
		object.put("key", "value");
		long position = store.put(object, new DBObjectSerializer());
		assert position > 0 : "Position should be greater than -1";
		DBObject result = store.get(position, new DBObjectSerializer());
		assertNotNull(result);
		assertEquals(object.get("_id"), result.get("_id"));

	}

	@Test
	public void testUpdate() throws Exception {
		BasicDBObject object = new BasicDBObject();
		object.put("_id", 1l);
		object.put("key", "value");
		long position = store.put(object, new DBObjectSerializer());
		assert position > 0 : "Position should be greater than -1";

		object.put("key", "new value");
		long newPosition = store.update(position, object, new DBObjectSerializer());

		assert newPosition == position : "Update should be at the same position";

		DBObject newObject = store.get(newPosition, new DBObjectSerializer());
		assertEquals(newObject.get("key"), "new value");

	}

	@Test
	public void testGetHeader() throws Exception {
		// We probably need more here
		Header header = store.getHeader();
		assert header.getPosition() > 0l : "Header is still at 0";
		assertEquals(DirectStore.class.getName(), header.getStore());
	}

	@Test
	public void tesRemove() throws Exception {
		store.remove(0);
	}

	@Test
	public void testCommit() throws Exception {
		// Does nothing
	}

	@Test
	public void testRollback() throws Exception {
		// Does nothing
	}

	@Test
	public void testIsClosed() throws Exception {
		assertFalse(store.isClosed());
		store.close();
		assertTrue(store.isClosed());
	}

	@Test
	public void testLock() throws Exception {
		// do nothing for now
	}

	@Test
	public void testUnlock() throws Exception {
		// do nothing for now
	}

	@Test
	public void testIsAppendOnly() throws Exception {
		assert !store.isAppendOnly(); // just to get line coverage
	}

	@Test
	public void testAlloc() throws Exception {
		// already tested
	}
}
