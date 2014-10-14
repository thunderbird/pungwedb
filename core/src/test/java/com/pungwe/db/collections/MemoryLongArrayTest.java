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
package com.pungwe.db.collections;

import com.pungwe.db.io.Memory;
import org.junit.*;

import java.util.Iterator;

import static org.junit.Assert.assertEquals;

/**
 * Created by ian on 04/10/2014.
 */
public class MemoryLongArrayTest {

	private Memory memory;

	@Before
	// 1MB is allocated
	public void setup() {
		memory = Memory.allocate(100000);
	}

	@After
	public void tearDown() {
		if (memory != null) {
			memory.free();
		}
	}

	@Test
	public void testAddSomeLongValues() throws Exception {
		MemoryLongArray array = new MemoryLongArray(memory);
		for (long i = 0; i < 10000; i++) {
			array.add(i);
		}

		for (long i = 0; i < 10000; i++) {
			long value = array.get(i);
			assertEquals(i, value);
		}
	}

	@Test
	public void testAddInMiddle() throws Exception {
		MemoryLongArray array = new MemoryLongArray(memory);
		for (long i = 0; i < 10; i++) {
			array.add(i);
		}
		assertEquals(10l, array.size());
		array.add(5l, 10l);
		assertEquals(11l, array.size());
		assertEquals(10l, (long)array.get(5l));
		assertEquals(5l, (long)array.get(6l));
		assertEquals(9l, (long)array.get(10l));
	}

	@Test
	public void testAddAtBeginning() throws Exception {
		MemoryLongArray array = new MemoryLongArray(memory);
		for (long i = 0; i < 10; i++) {
			array.add(i);
		}
		assertEquals(10l, array.size());
		array.add(0l, 10l);
		assertEquals(11l, array.size());
		assertEquals(10l, (long)array.get(0l));
		assertEquals(0l, (long)array.get(1l));
		assertEquals(9l, (long)array.get(10l));
	}

	@Test
	public void testAddOneBeforeEnd() throws Exception {
		MemoryLongArray array = new MemoryLongArray(memory);
		for (long i = 0; i < 10; i++) {
			array.add(i);
		}
		assertEquals(10l, array.size());
		array.add(9l, 10l);
		assertEquals(11l, array.size());
		assertEquals(10l, (long)array.get(9l));
		assertEquals(9l, (long)array.get(10l));
	}

	@Test
	public void testAddAtEnd() throws Exception {
		MemoryLongArray array = new MemoryLongArray(memory);
		for (long i = 0; i < 10; i++) {
			array.add(i);
		}
		assertEquals(10l, array.size());
		array.add(10l, 10l);
		assertEquals(11l, array.size());
		assertEquals(10l, (long)array.get(10l));
		assertEquals(9l, (long)array.get(9l));
	}

	@Test
	public void testRemoveAtIndex() throws Exception {
		MemoryLongArray array = new MemoryLongArray(memory);
		for (long i = 0; i < 10; i++) {
			array.add(i);
		}
		assertEquals(10l, array.size());
		array.remove(5l);
		assertEquals(9l, array.size());
		assertEquals(4l, (long)array.get(4l));
		assertEquals(6l, (long)array.get(5l));
	}
}
