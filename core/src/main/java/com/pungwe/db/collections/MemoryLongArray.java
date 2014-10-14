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

import java.util.*;
import java.util.function.Consumer;

/**
 * Created by ian on 04/10/2014.
 */
public class MemoryLongArray implements MemoryArray<Long>, Cloneable {

	// Memory is fluid...
	private final Memory memory;
	private long size = 0;

	public MemoryLongArray(Memory memory) {
		this.memory = memory;
	}

	private long offset(long index) {
		return index * 8;
	}

	@Override
	public synchronized void set(long index, Long value) {
		if (index > size) {
			throw new ArrayIndexOutOfBoundsException();
		}
		memory.setLong(offset(index), value);
	}

	@Override
	public synchronized void add(long index, Long value) {
		if (index < 0) {
			throw new IllegalArgumentException();
		}
		if (offset(size + 1) > memory.size()) {
			throw new ArrayIndexOutOfBoundsException();
		}
		if (index > 0 && index < size) {
			memory.put(offset(index + 1), memory, offset(index), memory.size());
			memory.setLong(offset(index), value);
		} else if (index == 0) {
			memory.put(offset(1), memory, 0, memory.size());
			memory.setLong(0, value);
		} else {
			memory.setLong(offset(index), value);
		}
		this.size++;
	}

	@Override
	public synchronized void add(Long value) {
		if (offset(size + 1) > memory.size()) {
			throw new ArrayIndexOutOfBoundsException();
		}
		memory.setLong(offset(size), value);
		size++;
	}

	@Override
	public synchronized void remove(long index) {
		if (index < 0) {
			throw new IllegalArgumentException();
		}
		if (size < index) {
			throw new ArrayIndexOutOfBoundsException();
		}
		long memorySize = memory.size();
		if (index == 0) {
			memory.put(0, memory, offset(1), memorySize);
		} else if (index < (size - 1)) {
			memory.put(offset(index), memory, offset(index + 1), memorySize);
		}
		size--;
	}

	@Override
	public synchronized Long get(long index) {
		if (index > size) {
			throw new ArrayIndexOutOfBoundsException();
		}
		return memory.getLong(offset(index));
	}

	public synchronized long size() {
		return size;
	}

	@Override
	public int hashCode() {
		return memory.hashCode();
	}

	@Override
	public boolean equals(Object obj) {
		return super.equals(obj);
	}

	@Override
	protected synchronized Object clone() throws CloneNotSupportedException {
		Memory copiedMemory = memory.copy(offset(size));
		return new MemoryLongArray(copiedMemory);
	}

	@Override
	public String toString() {
		return super.toString();
	}

	@Override
	public Iterator<Long> iterator() {
		return new LongArrayIterator();
	}

	private class LongArrayIterator implements Iterator<Long> {
		private long position = 0;

		@Override
		public synchronized boolean hasNext() {
			return position < size();
		}

		@Override
		public synchronized Long next() {
			return get(position++);
		}

		@Override
		public synchronized void remove() {
			MemoryLongArray.this.remove(position++);
		}
	}

}
