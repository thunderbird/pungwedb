package com.pungwe.db.types;

import com.pungwe.db.io.serializers.DBObjectSerializer;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.serializers.Serializers;
import com.pungwe.db.io.store.AppendOnlyStore;
import com.pungwe.db.io.store.DirectStore;
import com.pungwe.db.io.volume.MappedFileVolume;
import com.pungwe.db.io.volume.MemoryVolume;
import com.pungwe.db.io.volume.Volume;
import com.pungwe.db.types.btree.BTreeMap;
import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by 917903 on 02/03/2015.
 */
@Ignore
public class ShardedVolumeTest {

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
	public void testAddManyMultiThreaded() throws Exception {
		ExecutorService executor = Executors.newFixedThreadPool(8);

		try {
			System.out.println("Multi threaded");

			// Create array of trees
			final int shardSize = 8;
			final BTreeMap<Long, DBObject> tree[] = new BTreeMap[shardSize];
			for (int i = 0; i < tree.length; i++) {
				File file = File.createTempFile("tmp", "db");
				file.deleteOnExit();
				File recFile = File.createTempFile("tmp", "idx");
				file.deleteOnExit();
				Volume volume = new MappedFileVolume(file, false, 30);
				final Volume recVolume = new MappedFileVolume(recFile, false, 20);
				final AppendOnlyStore store = new AppendOnlyStore(volume, recVolume);
				tree[i] = new BTreeMap<Long, DBObject>(store, comp, keySerializer, valueSerializer, 100, true);
			}

			Collection<Callable<Long>> threads = new LinkedList<>();
			for (int i = 0; i < 1000000; i++) {
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

							tree[Long.hashCode(key) % shardSize].put(key, object);

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

			System.out.println("It took: " + ((end - start) / 1000000000d) + " seconds to save and index " + 100000);

			start = System.nanoTime();
			// Validate that every element is in the datastore
			for (int i = 0; i < 1000000; i++) {
				try {
					DBObject get = tree[Long.hashCode(i) % shardSize].get((long) i);
					assertNotNull("null get: i (" + i + ")", get);
					assertEquals((long) i, get.get("_id"));
				} catch (Throwable ex) {
					System.out.println("Failed at record: " + i);
					throw ex;
				}
			}
			end = System.nanoTime();
			System.out.println("It took: " + ((end - start) / 1000000000d) + " seconds to bulk read " + 100000 + ": ");

			for (BTreeMap<Long, DBObject> t : tree) {
				System.out.println("Entries per tree:" + t.size());
			}
		} finally {
			executor.shutdown();
		}
	}
}
