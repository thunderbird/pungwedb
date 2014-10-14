/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.pungwe.db.io;

import com.pungwe.db.io.serializers.BasicDBObjectSerializer;
import com.pungwe.db.types.BasicDBObject;
import org.joda.time.DateTime;
import org.junit.Test;

import java.io.*;
import java.util.Date;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Created by ian on 30/09/2014.
 */
public class RandomAccessFileStoreTest {

	@Test
	public void testPutObjectAndRead() throws Exception {
		BasicDBObject object = new BasicDBObject();
		object.put("_id", UUID.randomUUID().toString());
		object.put("string", "string value");
		object.put("number", 1.0d);
		object.put("date", new Date());

		// Create tmp file
		File file = File.createTempFile(UUID.randomUUID().toString(), "");
		RandomAccessFileStore store = new RandomAccessFileStore(file);

		// Serialize to an output stream

		long position = store.put(object, new BasicDBObjectSerializer());

		BasicDBObject result = store.get(position, new BasicDBObjectSerializer());

		assertEquals(object.get("_id"), result.get("_id"));
		assertEquals(object.get("string"), result.get("string"));
		assertEquals(object.get("number"), result.get("number"));
		assertTrue(result.get("date") instanceof DateTime);
		assertEquals(object.get("date"), ((DateTime) result.get("date")).toDate());

		store.close();
		file.deleteOnExit();
	}

	@Test
	public void testMultiPutGetObject() throws Exception {

		// Create tmp file
		File file = File.createTempFile(UUID.randomUUID().toString(), "");
		RandomAccessFileStore store = new RandomAccessFileStore(file);

		final AtomicBoolean errors = new AtomicBoolean(false);
		final AtomicInteger threads = new AtomicInteger(1000);
		final Object lock = new Object();
		long start = System.nanoTime();
		for (long i = 0; i < 1000; i++) {
			final long number = i;
			Runnable run = new Runnable() {
				public void run() {
					// Serialize to an output stream
					BasicDBObject object = new BasicDBObject();
					object.put("_id", UUID.randomUUID().toString());
					object.put("string", "string value");
					object.put("number", number);
					object.put("date", new Date());

					try {
						long position = store.put(object, new BasicDBObjectSerializer());

						BasicDBObject result = store.get(position, new BasicDBObjectSerializer());

						assertEquals(object.get("_id"), result.get("_id"));
						assertEquals(object.get("string"), result.get("string"));
						assertEquals(object.get("number"), result.get("number"));
						assertTrue(result.get("date") instanceof DateTime);
						assertEquals(object.get("date"), ((DateTime) result.get("date")).toDate());
					} catch (Exception ex) {
						errors.set(true);
					}

					synchronized (lock) {
						threads.decrementAndGet();
						lock.notifyAll();
					}
				}
			};
			new Thread(run).start();
		}
		synchronized (lock) {
			while (threads.get() > 0) {
				lock.wait();
			}
		}
		long end = System.nanoTime();

		System.out.println("It took " + ((end - start) / 1000000000) + "s to run");
		assertFalse(errors.get());

		store.close();
		file.deleteOnExit();
	}
}
