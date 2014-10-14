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
package com.pungwe.db.types;

import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.exception.DuplicateKeyException;
import com.pungwe.db.io.Store;
import com.pungwe.db.io.serializers.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

// FIXME: This needs to be a counting BTree...
/**
 * Created by ian on 03/10/2014.
 */
public class BTree<K,V> implements Iterable<BTree<K,V>>, Lockable<Long> {

	private static final Logger log = LoggerFactory.getLogger(BTree.class);

	private final ConcurrentMap<Long, Thread> locks = new ConcurrentHashMap<>();

	private final Store store;
	private final Serializer<K> keySerializer;
	private final Serializer<V> valueSerializer;
	private final Serializer<BTreeNode> nodeSerializer;

	private final Comparator<K> comparator;
	private final String database;
	private final String collection;
	private final String name;

	private final boolean unique;
	private final boolean dropDups;
	private final int maxNodeSize;
	private final boolean referencedValue;


	private Reference rootPointer;

	// Used for creating a BTree
	public BTree(Store store, Comparator<K> comparator, Serializer<K> keySerializer, Serializer<V> valueSerializer, String database,
	             String collection, String name, boolean unique, boolean dropDups, int maxNodeSize, boolean referencedValue) {
		this(store, null, comparator, keySerializer, valueSerializer, database, collection, name, unique, dropDups, maxNodeSize, referencedValue);
	}

	public BTree(Store store, Reference pointer, Comparator<K> comparator, Serializer<K> keySerializer, Serializer<V> valueSerializer, String database,
	             String collection, String name, boolean unique, boolean dropDups, int maxNodeSize, boolean referencedValue) {
		this.database = database;
		this.collection = collection;
		this.name = name;
		this.unique = unique;
		this.dropDups = dropDups;
		this.comparator = comparator;
		this.store = store;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.nodeSerializer = new BTreeNodeSerializer();
		this.referencedValue = referencedValue;
		this.maxNodeSize = maxNodeSize;

		// This can be null and can change...
		this.rootPointer = pointer;
	}

	public V get(Object key) {
		if (key == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (rootPointer == null) {
			return null;
		}
		try {
			return findValueByKey(rootPointer.pointer, (K)key);
		} catch (IOException ex) {
			throw new NoSuchElementException("No such key: " + key);
		}
	}

	private V findValueByKey(long np, K key) throws IOException {
		// Load the node at np.
		BTreeNode node = store.get(np, nodeSerializer);
		// If the node is a dir node, we need to find the child
		int pos = findChildren(key, node.keys());
		if (pos < 0) {
			pos = 0;
		}
		if (node instanceof BTree.DirNode) {
			return findValueByKey(((DirNode) node).children()[pos], key);
		} else if (node instanceof BTree.LeafNode) {
			if (node.keys()[pos] != null && 0 == comparator.compare(key, (K)node.keys()[0])) {
				Object value = node.values()[pos];
				if (value instanceof BTree.Reference) {
					return (V) store.get(((Reference) value).pointer, valueSerializer);
				} else {
					return (V) value; // should be of the correct type...
				}
			}
		}
		// Return null if nothing is found...
		return null;
	}

	private int findChildren(final Object key, final Object[] keys) {
		int left = 0;
		if (keys[0] == null) {
			left++;
		}
		int right = keys[keys.length-1] == null ? keys.length - 1 : keys.length;
		int middle = 0;
		// Binary Search
		while (true) {
			middle = (left + right) / 2;
			if (keys[middle] == null) {
				return middle;
			} else {
				right = middle;
			}
			if (left >= right) {
				return right;
			}
		}
	}

	public V add(K key, V value) {
		if (key == null && value == null) {
			throw new IllegalArgumentException("Both key and value cannot be null");
		}
		if (value == null) {
			throw new IllegalArgumentException("Value cannot be null");
		}

		return value;
	}

	public V update(K key, V value) {
		return value;
	}

	private V insertEntry(final K key, final V v) throws IOException, DuplicateKeyException {
		V value = v;
		if (referencedValue) {
			long p = store.put((V)v, valueSerializer);
			value = (V)new Reference(p); // Don't you love generics
		}

		BTreeNode node = null;
		if (rootPointer == null) {
			node = new LeafNode(new Object[1], new Object[1], 0);
		} else {
			node = store.get(rootPointer.pointer, nodeSerializer);
		}
		int stackPosition = -1;
		long[] stackPositions = new long[4];
		// Loop until we find a leaf node to insert into
		long currentNode = -1;
		while (!(node instanceof BTree.LeafNode)) {
			int pos = findChildren(key, node.keys());
			if (pos < 0) {
				pos = 0;
			}
			currentNode = node.children()[pos];
			node = store.get(currentNode, nodeSerializer);
			stackPosition++;
			// Grow the array if needed
			if (stackPositions.length == stackPosition) {
				stackPositions = Arrays.copyOf(stackPositions, stackPositions.length * 2);
			}
			stackPositions[stackPosition] = currentNode;
		}

		try {
			// Lock the node to the current thread
			lock(currentNode);
			int pos = findChildren(key, node.keys());

			// If the key exists, we need to determine if it's a single value key or a multi value key
			// if it's a single value key, we need to check for uniqueness. If it dropDups is true, then we simply
			// replace the existing key with the new one.
			if (pos < (node.keys().length - 1) && node.keys()[pos] != null && comparator.compare(key, (K)node.keys()[pos]) == 0) {
				// If unique and not drop keys
				if (unique && !dropDups) {
					throw new DuplicateKeyException("Key :" + key + " already exists");
				}

				// Drop the existing key and replace it with the new one
				if (unique && dropDups) {
					node.values()[pos] = v;
				}

				if (!unique) {
					// FIXME: Write this bit
				}

				// Write the node to disk.
				long newNode = store.update(currentNode, node, nodeSerializer);

				// If the newPosition does not match the old position, the node has been moved in the file:
				if (newNode != currentNode) {
					
				}
			}
		} finally {
			unlock(currentNode);
		}
		return (V)v;
	}

	@Override
	public Iterator<BTree<K, V>> iterator() {
		return new BTreeIterator();
	}

	@Override
	public void lock(Long v) {
		final Thread t = Thread.currentThread();

		if (locks.get(v) == t) {
			log.warn("Lock " + v + " already held by thread " + t.getId());
			return;
		}

		// Attempt to get the lock. If it's not available wait 10 nanoseconds...
		while (locks.putIfAbsent(v, t) != null) {
			LockSupport.parkNanos(10);
		}
	}

	@Override
	public void unlockAll(Long v) {
		// do nothing...
	}

	@Override
	public void unlock(Long v) {
		final Thread t = locks.remove(v);
	}

	public final class Reference {

		final long pointer;

		public Reference(long pointer) {
			this.pointer = pointer;
		}

		@Override
		public boolean equals(Object o) {
			throw new IllegalAccessError();
		}

		@Override
		public int hashCode() {
			throw new IllegalAccessError();
		}

		@Override
		public String toString() {
			return "BTree Reference{" +
					"pointer=" + pointer +
					'}';
		}
	}

	public interface BTreeNode {
		Object[] keys();
		Object[] values();
		long[] children();
		long next();
		boolean isLeaf();
	}

	public final class DirNode implements BTreeNode {
		final Object[] keys;
		final long[] children;

		public DirNode(Object[] keys, long[] children) {
			this.keys = keys;
			this.children = children;
		}

		public DirNode(Object[] keys, List<Long> children) {
			this.keys = keys;
			this.children = new long[children.size()];
			for (int i = 0; i < children.size(); i++) {
				this.children[i] = children.get(i);
			}
		}

		@Override
		public Object[] keys() {
			return keys;
		}

		@Override
		public Object[] values() {
			return null;
		}

		@Override
		public long[] children() {
			return children;
		}

		public long next() {
			return children[children.length - 1];
		}

		@Override
		public boolean isLeaf() {
			return false;
		}

		@Override
		public String toString() {
			return "DirNode{" +
					"keys=" + Arrays.toString(keys) +
					", children=" + Arrays.toString(children) +
					'}';
		}
	}

	public final class LeafNode implements BTreeNode {
		Object[] keys;
		Object[] values;
		final long next;

		public LeafNode(Object[] keys, Object[] values, long next) {
			this.keys = keys;
			this.values = values;
			this.next = next;
		}

		@Override
		public Object[] keys() {
			return keys;
		}

		@Override
		public Object[] values() {
			return values;
		}

		@Override
		public long[] children() {
			return null;
		}

		@Override
		public long next() {
			return next;
		}

		@Override
		public boolean isLeaf() {
			return true;
		}
	}

	// TODO
	public final class BTreeIterator implements Iterator<BTree<K,V>> {

			@Override
		public boolean hasNext() {
			return false;
		}

		@Override
		public BTree<K, V> next() {
			return null;
		}

	}

	public final class BTreeNodeSerializer implements Serializer<BTreeNode> {

		public BTreeNodeSerializer() {

		}

		@Override
		public void serialize(DataOutput out, BTreeNode value) throws IOException {

		}

		@Override
		public BTreeNode deserialize(DataInput in) throws IOException {
			return null;
		}

		@Override
		public TypeReference getTypeReference() {
			return null;
		}
	}
}
