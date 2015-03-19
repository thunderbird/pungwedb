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
package com.pungwe.db.util.collections;

import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.store.Store;

import java.io.IOException;
import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by ian on 17/03/2015.
 */
public final class DBQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {

	private static final long TIMEOUT = 10000; // 10 nanoseconds
	protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	protected final Store store;
	protected final Serializer<E> serializer;
	protected final AtomicLong headRecId = new AtomicLong();
	protected final AtomicLong nextRecordId = new AtomicLong();
	protected final AtomicLong size = new AtomicLong();
	protected final long maxSize;

	public DBQueue(Store store, Serializer<E> serializer, long headRecId, long size, long maxSize) throws IOException {
		this.store = store;
		this.headRecId.set(headRecId <= 0 ? store.getFirstId() : headRecId);
		this.nextRecordId.set(store.getNextId());
		this.serializer = serializer;
		this.size.set(size);
		this.maxSize = maxSize;
	}

	public DBQueue(Store store, Serializer<E> serializer, long headRecId, long size) throws IOException {
		this(store, serializer, headRecId, size, -1);
	}

	public DBQueue(Store store, Serializer<E> serializer, long headRecId) throws IOException {
		this(store, serializer, headRecId, 0, -1l);
	}

	public DBQueue(Store store, Serializer<E> serializer) throws IOException {
		this(store, serializer, 0, 0, -1l);
	}

	@Override
	public Iterator<E> iterator() {
		return null;
	}

	@Override
	public int size() {
		return (int) Math.min(sizeLong(), Integer.MAX_VALUE);
	}

	public long sizeLong() {
		return size.get();
	}

	@Override
	public void put(E e) throws InterruptedException {
		while (!offer(e, TIMEOUT, TimeUnit.NANOSECONDS));
	}

	@Override
	public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
		if(offer(e)) return true;
		long target = System.currentTimeMillis() + unit.toMillis(timeout);
		while(target>=System.currentTimeMillis()){
			if(offer(e))
				return true;
			LockSupport.parkNanos(TIMEOUT);
		}

		return false;
	}

	@Override
	public E take() throws InterruptedException {
		E value = null;
		while ((value = poll(TIMEOUT, TimeUnit.NANOSECONDS)) == null);
		return value;
	}

	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {

		long target = System.currentTimeMillis() + unit.toMillis(timeout);
		while(target>=System.currentTimeMillis()){
			E value = poll();
			if (value != null) {
				return value;
			}
			LockSupport.parkNanos(TIMEOUT);
		}
		return null;
	}

	@Override
	public int remainingCapacity() {
		return (int)Math.min(remainingCapacityLong(), Integer.MAX_VALUE);
	}

	public long remainingCapacityLong() {
		return maxSize - size.get();
	}

	@Override
	public int drainTo(Collection<? super E> c) {
		return 0;
	}

	@Override
	public int drainTo(Collection<? super E> c, int maxElements) {
		return 0;
	}

	@Override
	public boolean offer(E e) {
		if (maxSize > 0 && remainingCapacity() <= 0) {
			return false;
		}
		lock.writeLock().lock();
		try {
			final long recordId = nextRecordId.getAndSet(store.getNextId());
			final long nextId = nextRecordId.get();
			DBQueueNode<E> node = new DBQueueNode<>();
			node.setTimestamp(System.nanoTime());
			node.setNext(nextId);
			node.setValue(e);
			store.update(recordId, node, new DBQueueNode.NodeSerializer<>(serializer));

			return true;
		} catch (IOException ex) {
			return false;
		} finally {
			if (lock.writeLock().isHeldByCurrentThread()) {
				lock.writeLock().unlock();
			}
		}
	}

	@Override
	public E poll() {
		lock.writeLock().lock();
		try {
			DBQueueNode<E> node = store.get(headRecId.get(), new DBQueueNode.NodeSerializer<>(serializer));
			if (node == null) {
				return null; // no new record
			}
			// Remove the record
			store.remove(headRecId.get());
			// Set to the next record id.
			headRecId.set(node.getNext());
			return node.getValue();
		} catch (IOException e) {
			return null;
		} finally {
			if (lock.writeLock().isHeldByCurrentThread()) {
				lock.writeLock().unlock();
			}
		}
	}

	@Override
	public E peek() {
		lock.readLock().lock();
		try {
			DBQueueNode<E> node = store.get(headRecId.get(), new DBQueueNode.NodeSerializer<>(serializer));
			return node == null ? null : node.getValue();
		} catch (IOException ex) {
			return null;
		} finally {
			lock.readLock().unlock();
		}
	}

	static class DBQueueIterator<E1> implements Iterator<E1> {

		@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public E1 next() {
			return null;
		}

		@Override
		public void remove() {

		}

	}
}