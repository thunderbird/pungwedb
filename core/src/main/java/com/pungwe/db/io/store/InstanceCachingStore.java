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
package com.pungwe.db.io.store;

import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.types.Header;
import org.apache.commons.collections4.MapIterator;
import org.apache.commons.collections4.map.LRUMap;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ian on 09/03/2015.
 */
public class InstanceCachingStore implements Store {

	final Store store;
	private final int cacheSize;
	private volatile LRUMap<Long, Record<?>> cache;
	private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	public InstanceCachingStore(Store store, int cacheSize) {
		this.store = store;
		this.cacheSize = cacheSize;
		cache = new LRUMap<>(cacheSize);
	}

	@Override
	public <T> long put(T value, Serializer<T> serializer) throws IOException {

		lock.writeLock().lock();
		try {
			// Get a new position
			if (cache.isFull()) {
				flushCache();
			}
			long recId = getNextId();
			Record rec = new Record();
			rec.value = value;
			rec.serializer = serializer;
			cache.put(recId, rec);
			return recId;
		} finally {
			if (lock.writeLock().isHeldByCurrentThread()) {
				lock.writeLock().unlock();
			}
		}
	}

	@Override
	public <T> T get(long position, Serializer<T> serializer) throws IOException {
		// Get never adds to the cache, it only checks if the value is there...
		try {
			lock.readLock().lock();
			if (cache.containsKey(position)) {
				T value = (T)cache.get(position).value;
				return value;
			}
			return store.get(position, serializer);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public <T> long update(long position, T value, Serializer<T> serializer) throws IOException {
		lock.writeLock().lock();
		try {
			// Get a new position
			if (cache.isFull()) {
				flushCache();
			}
			Record rec = new Record();
			rec.value = value;
			rec.serializer = serializer;
			cache.put(position, rec);
			return position;
		} finally {
			if (lock.writeLock().isHeldByCurrentThread()) {
				lock.writeLock().unlock();
			}
		}
	}

	private void flushCache() throws IOException {
		LRUMap<Long, Record<?>> current = cache;
		cache = new LRUMap<Long, Record<?>>(cacheSize);
		MapIterator<Long, Record<?>> it = current.mapIterator();
		while (it.hasNext()) {
			Long key = it.next();
			Record<Object> value = (Record<Object>)it.getValue();
			store.update(key, value.value, value.serializer);
		}
	}

	@Override
	public Header getHeader() {
		return store.getHeader();
	}

	@Override
	public void remove(long position) throws IOException {

	}

	@Override
	public void commit() throws IOException {
		flushCache(); // force flush
		store.commit();
	}

	@Override
	public void rollback() throws UnsupportedOperationException, IOException {

	}

	@Override
	public boolean isClosed() throws IOException {
		return false;
	}

	@Override
	public void lock(long position, int size) {

	}

	@Override
	public void unlock(long position, int size) {

	}

	@Override
	public boolean isAppendOnly() {
		return store.isAppendOnly();
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public long getNextId() throws IOException {
		return store.getNextId();
	}

	private static class Record<T> {
		private volatile T value;
		private Serializer<T> serializer;
	}
}
