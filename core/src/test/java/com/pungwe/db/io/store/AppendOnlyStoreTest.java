package com.pungwe.db.io.store;

import com.pungwe.db.io.serializers.DBDocumentSerializer;
import com.pungwe.db.io.volume.MemoryVolume;
import com.pungwe.db.io.volume.Volume;
import com.pungwe.db.types.*;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by 917903 on 13/02/2015.
 */
public class AppendOnlyStoreTest {

	Volume volume;
	Volume recVolume;
	AppendOnlyStore store;

	@Before
	public void setupBuffer() throws Exception {
		volume = new MemoryVolume(false);
		recVolume = new MemoryVolume(false);
		store = new AppendOnlyStore(volume, recVolume);
	}

	@Test
	public void testPut() throws Exception {
		BasicDBDocument object = new BasicDBDocument();
		object.put("_id", 1l);
		object.put("key", "value");
		long position = store.put(object, new DBDocumentSerializer());
		assert position > -1 : "Position should be greater than -1";

	}

	@Test
	public void testGet() throws Exception {
		BasicDBDocument object = new BasicDBDocument();
		object.put("_id", 1l);
		object.put("key", "value");
		long position = store.put(object, new DBDocumentSerializer());
		assert position > -1 : "Position should be greater than -1";
		DBDocument result = store.get(position, new DBDocumentSerializer());
		assertNotNull(result);
		assertEquals(object.get("_id"), result.get("_id"));

	}

	@Test
	public void testUpdate() throws Exception {
		BasicDBDocument object = new BasicDBDocument();
		object.put("_id", 1l);
		object.put("key", "value");
		long position = store.put(object, new DBDocumentSerializer());
		assert position > -1 : "Position should be greater than -1";

		DBDocument old = store.get(position, new DBDocumentSerializer());
		assertEquals(old.get("key"), "value");

		object.put("key", "new value");
		long newPosition = store.update(position, object, new DBDocumentSerializer());

		DBDocument newObject = store.get(newPosition, new DBDocumentSerializer());
		assertEquals(newObject.get("key"), "new value");

	}

	@Test
	public void testGetHeader() throws Exception {
		// We probably need more here
		Header header = store.getHeader();
		assert header.getPosition() > 0l : "Header is still at 0";
		assertEquals(header.getStore(), AppendOnlyStore.class.getName());
	}

	@Test
	public void tesRemove() throws Exception {
		store.remove(0); // Doesn't make any difference
	}

	@Test
	public void testCommit() throws Exception {
		BasicDBDocument object = new BasicDBDocument();
		object.put("_id", 1l);
		object.put("key", "value");
		long position = store.put(object, new DBDocumentSerializer());
		assert position > 0 : "Position should be greater than -1";

		Header header = store.getHeader();
		header.setMetaData(position);

		store.commit();

		System.out.println((double)volume.getLength() / 1024 / 1024 + "MB");

		Header writtenHeader = store.findHeader();

		assertNotEquals(writtenHeader.getPosition(), position);
	}

	// FIXME: Sort this out... rollback won't work with new recid method
	@Test
	@Ignore
	public void testRollback() throws Exception {

		BasicDBDocument object = new BasicDBDocument();
		object.put("_id", 1l);
		object.put("key", "value");
		long position = store.put(object, new DBDocumentSerializer());
		assert position > -1 : "Position should be greater than -1";
		Header header = store.getHeader();
		header.setMetaData(position);
		store.commit();
		object.put("key", "value to rollback");
		long newPositon = store.update(position, object, new DBDocumentSerializer());

		assertEquals("value to rollback", store.get(newPositon, new DBDocumentSerializer()).get("key"));

		header.setMetaData(newPositon);

		store.rollback();

		assertEquals("value", store.get(newPositon, new DBDocumentSerializer()).get("key"));
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
		assert store.isAppendOnly(); // just to get line coverage
	}

	@Test
	public void testAlloc() throws Exception {
		// already tested
	}
}
