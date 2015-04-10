package com.pungwe.db.util.collections;

import com.pungwe.db.io.store.AppendOnlyStore;
import com.pungwe.db.io.store.DirectStore;
import com.pungwe.db.io.store.InstanceCachingStore;
import com.pungwe.db.io.store.Store;
import com.pungwe.db.io.volume.MappedFileVolume;
import com.pungwe.db.io.volume.MemoryVolume;
import com.pungwe.db.io.volume.RandomAccessFileVolume;
import com.pungwe.db.io.volume.Volume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Iterator;

/**
 * Created by 917903 on 30/03/2015.
 */
public class LinkedLongArrayTest {

	private Volume storeVolume;
	private Volume recIdVolume;
	private Store store = null;

	@Before
	public void setup() throws Exception {
		File tempStore = File.createTempFile("tmp", "db");
		tempStore.deleteOnExit();
		File tempRecIdStore = File.createTempFile("tmp", "idx");
		tempRecIdStore.deleteOnExit();

//		storeVolume = new MemoryVolume(false, 30);
//		recIdVolume = new MemoryVolume(false);

		storeVolume = new RandomAccessFileVolume(tempStore, false);
		recIdVolume = new MappedFileVolume(tempRecIdStore, false, 20);

		store = new InstanceCachingStore(new AppendOnlyStore(storeVolume, recIdVolume), 10000);
	}
	@Test
	public void testAddLongsAndIterate() throws Exception {
		LinkedLongArray array = new LinkedLongArray(store);
		for (long i = 0; i < 1000; i++) {
			array.add(i);
		}

		Iterator<Long> it = array.iterator();
		int i = 0;
		while (it.hasNext()) {
			long n = it.next();
			assert (long)i == n : " Expected: " + i + " but got: " + n;
			i++;
		}
		assert i == 1000 : "Expected 1000 but was: " + i;
	}

	@Test
	public void testAddLongsAndGet() throws Exception {
		LinkedLongArray array = new LinkedLongArray(store);
		for (long i = 0; i < 1000; i++) {
			array.add(i);
		}

		long value = array.get(789l);

		assert value == 789l : "Value did not match 7898l was " + value;
	}
}
