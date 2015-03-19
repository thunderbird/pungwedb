package com.pungwe.db.util.collections;

import com.pungwe.db.io.serializers.DBDocumentSerializer;
import com.pungwe.db.io.serializers.Serializers;
import com.pungwe.db.io.store.DirectStore;
import com.pungwe.db.io.store.Store;
import com.pungwe.db.io.volume.MemoryVolume;
import com.pungwe.db.io.volume.Volume;
import com.pungwe.db.types.BasicDBDocument;
import com.pungwe.db.types.DBDocument;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Created by 917903 on 18/03/2015.
 */
public class DBQueueTest {

	private Volume storeVolume;
	private Volume recIdVolume;
	private Store store = null;

	@Before
	public void setup() throws Exception {
		storeVolume = new MemoryVolume(false, 30);
		recIdVolume = new MemoryVolume(false);
		store = new DirectStore(storeVolume, recIdVolume);
	}

	@Test
	public void testPutPoll() throws Exception {
		DBQueue<String> queue = new DBQueue<String>(store, new Serializers.STRING());
		queue.put("Hello World");
		String result = queue.poll();

		assertEquals("Hello World", result);
	}

	@Test
	public void testMultiPutPoll() throws Exception {

		DBQueue<DBDocument> queue = new DBQueue<DBDocument>(store, new DBDocumentSerializer());

		byte[] b = new byte[4096];

		for (int j = 0; j < 4096; j++) {
			b[j] = (byte)j;
		}

		long start = System.nanoTime();

		for (int i = 0; i < 100000; i++) {
			DBDocument d = new BasicDBDocument();
			d.put("_id", (long)i);
			d.put("firstname", "Ian");
			d.put("surname", "Michell");
			d.put("initials", "C");
			d.put("title", "Mr");
			d.put("data", b);

			queue.put(d);
		}
		long end = System.nanoTime();

		System.out.println("Took: " + ((double)(end - start) / 1000000000d) + "s to queue");

		start = System.nanoTime();
		for (int i = 0; i < 100000; i++) {
			DBDocument value = queue.poll();
			assertEquals((long)i, value.get("_id"));
			assertEquals((byte)3000, ((byte[])value.get("data"))[3000]);
		}
		end = System.nanoTime();

		System.out.println("Took: " + ((double)(end - start) / 1000000000d) + "s to dequeue");
	}
}
