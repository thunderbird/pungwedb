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

import com.pungwe.db.io.Allocator;
import sun.misc.Unsafe;

import java.lang.reflect.Field;

/**
 * Created by ian on 04/10/2014.
 */
// Borrowed from Cassandra
public class NativeAllocator implements Allocator {

	protected static final Unsafe unsafe;

	static {
		try {
			Field field = Unsafe.class.getDeclaredField("theUnsafe");
			field.setAccessible(true);
			unsafe = (Unsafe)field.get(null);
		} catch (Exception ex) {
			throw new AssertionError(ex);
		}
	}

	public synchronized long allocate(long size) {
		return unsafe.allocateMemory(size);
	}

	public synchronized void free(long peer) {
		unsafe.freeMemory(peer);
	}

	public synchronized long reallocate(long peer, long size) {
		return unsafe.reallocateMemory(peer, size);
	}
}
