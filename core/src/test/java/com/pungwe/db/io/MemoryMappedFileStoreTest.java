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

import com.pungwe.db.io.serializers.DBObjectSerializer;
import com.pungwe.db.types.BasicDBObject;
import com.pungwe.db.types.DBObject;
import com.pungwe.db.types.Header;
import org.junit.Test;

import java.io.File;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Created by ian on 29/10/2014.
 */
public class MemoryMappedFileStoreTest {

	@Test
	public void testCreateStoreAndWriteHeaders() throws Exception {
		File file = File.createTempFile("mmap", "");
		try {
			MemoryMappedFileStore store = new MemoryMappedFileStore(file, 8096, -1);
			store.close();
			store = new MemoryMappedFileStore(file, -1, -1);
			Header header = store.getHeader();
			assertEquals(4096, header.getBlockSize());
			assertEquals(4096, header.getPosition());
			assertEquals(MemoryMappedFileStore.class.getName(), header.getStore());
			assertEquals(0, header.getMetaData());
		} finally {
			file.delete();
		}
	}

	@Test
	public void testReadWriteData() throws Exception {
		String id = UUID.randomUUID().toString();
		DBObject object = new BasicDBObject();
		object.put("_id", id);
		object.put("value", "My Value");
		File file = File.createTempFile("mmap", "");
		try {
			MemoryMappedFileStore store = new MemoryMappedFileStore(file, 16 * 1024, -1);
			long p = store.put(object, new DBObjectSerializer());
			store.close();
			store = new MemoryMappedFileStore(file, -1, -1);
			// Assert Headers
			Header header = store.getHeader();
			assertEquals(4096, header.getBlockSize());
			assertEquals(8192, header.getPosition());
			assertEquals(MemoryMappedFileStore.class.getName(), header.getStore());
			assertEquals(0, header.getMetaData());

			DBObject result = store.get(p, new DBObjectSerializer());
			assertEquals(object.get("_id"), result.get("_id"));
			assertEquals(object.get("value"), result.get("value"));
		} finally {
			 file.delete();
		}

	}
}
