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

import com.pungwe.db.types.Header;
import org.junit.Test;

import java.io.File;

import static org.junit.Assert.assertEquals;

/**
 * Created by ian on 29/10/2014.
 */
public class MemoryMappedFileStoreTest {

	@Test
	public void testCreateStoreAndWriteHeaders() throws Exception {
		File file = File.createTempFile("mmap", "");
		try {
			MemoryMappedFileStore store = new MemoryMappedFileStore(file, 256 * 1024 * 1024, -1);
			store.close();
			store = new MemoryMappedFileStore(file, -1, -1);
			Header header = store.getHeader();
			assertEquals(4096, header.getBlockSize());
			assertEquals(4096, header.getNextPosition());
			assertEquals(MemoryMappedFileStore.class.getName(), header.getStore());
		} finally {
			file.delete();
		}
	}
}
