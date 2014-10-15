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
import java.util.concurrent.locks.LockSupport;

// FIXME: This needs to be a counting BTree...
// TODO: Add node size increments
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


	private Pointer rootPointer;

	// Used for creating a BTree
	public BTree(Store store, Comparator<K> comparator, Serializer<K> keySerializer, Serializer<V> valueSerializer, String database,
	             String collection, String name, boolean unique, boolean dropDups, int maxNodeSize, boolean referencedValue) {
		this(store, null, comparator, keySerializer, valueSerializer, database, collection, name, unique, dropDups, maxNodeSize, referencedValue);
	}

	public BTree(Store store, Pointer pointer, Comparator<K> comparator, Serializer<K> keySerializer, Serializer<V> valueSerializer, String database,
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
//		try {
//			//return findValueByKey(rootPointer.pointer, (K)key);
//			return null;
//		} catch (IOException ex) {
//			throw new NoSuchElementException("No such key: " + key);
//		}
		return null;
	}

	// TODO: Fix add, as it assumes unique and no duplicate dropping
	public V add(K key, V value) throws IOException, DuplicateKeyException {
		if (key == null && value == null) {
			throw new IllegalArgumentException("Both key and value cannot be null");
		}
		if (value == null) {
			throw new IllegalArgumentException("Value cannot be null");
		}

		// Cheating on generics a bit
		V v = value;
		if (referencedValue) {
			long p = store.put(value, valueSerializer);
			v = (V)new Pointer(p);
		}

		// Is the root node null?
		if (rootPointer == null) {
			BTreeNode root = new BTreeNode(maxNodeSize, true);
			BTreeEntry entry = new BTreeEntry();
			entry.setKey(key);
			entry.setValue(v);
			root.getEntries().add(entry);

			long p = store.put(root, nodeSerializer);
			rootPointer = new Pointer(p); // Store this for later
			return value;
		}

		// Root exists
		BTreeNode node = store.get(rootPointer.pointer, nodeSerializer);
		// Always add root to the stack
		List<BTreeNode> nodes = new ArrayList<>(4);
		nodes.add(node);
		// Find the leaf node
		while (!node.isLeaf()) {
			int pos = determinePosition(key, node.getEntries());
			BTreeEntry entry = node.getEntries().get(pos);
			if (entry.getLeft() != null && comparator.compare(key, (K)entry.getKey()) < 0) {
				node = store.get(entry.getLeft().pointer, nodeSerializer);
			} else {
				node = store.get(entry.getRight().pointer, nodeSerializer);
			}
			// Push to beginning of array list
			nodes.add(0, node);
		}

		// Find position to insert
		int pos = determinePosition(key, node.getEntries());

//		// if the key is null, then simple add it at that position
//		if (node.getEntries().size() > 0 && node.getEntries().size() < pos) {
//			node.keys()[pos] = key;
//			node.values()[pos] = v;
//			updateNodes(nodes, stack);
//			return value;
//		}

		K found = (K)node.keys()[pos];
		// If the key is the same, then check the unique value
		// TODO: Support dups
		int comp = comparator.compare(key, found);
		if (comp == 0) {
			if (unique && !dropDups) {
				throw new DuplicateKeyException("Key: " + key + " is not unique");
			}
			if (unique && dropDups) {
				node.values()[pos] = v;
				updateNodes(nodes, stack);
				return value;
			}
			if (!unique) {
				throw new UnsupportedOperationException("Non-unique not supported yet");
			}
		}

		// We need to handle this somehow as it take a value comparison to do so.
		Object[] newKeys = new Object[node.keys().length + 1];
		Object[] newValues = new Object[node.values().length + 1];

		// We need to expand the key / value arrays and insert at the beginning...
		if (comp < 0) {
			//System.arraycopy(elementData, index, elementData, index + 1,
			//size - index);
			//elementData[index] = element;
			System.arraycopy(node.keys(), pos > 0 ? pos : 0, newKeys, pos + 1, node.keys().length);
			newKeys[pos] = key;
		}

		LeafNode newLeaf = new LeafNode(newKeys, newValues, 0);
		nodes.set(0, newLeaf);
		updateNodes(nodes, stack);

		return value;
	}

	private void updateNodes(List<BTreeNode> nodes, List<Long> stack) throws IOException {
		// Store the first node in the list...
		long newPointer = store.put(nodes.get(0), nodeSerializer);
		long current = stack.get(0);
		if (newPointer == current) {
			// no need to change anything here as we are updating directly and it hasn't moved...
			return;
		}
		// Iterate every node after that...
		for (int i = 1; i < nodes.size(); i++) {
			BTreeNode node = nodes.get(i); // parent node
			int childPos = findChildByPointer(current, node.children());
			node.children()[childPos] = newPointer;
			newPointer = store.put(node, nodeSerializer);
			current = stack.get(i);
			if (newPointer == current) {
				return;
			}
		}
	}

	private int findChildByPointer(long p, long[] children) {
		int left = 0;
		int right = children.length - 1;
		int middle = right / 2;

		while (true) {
			if (left >= right) {
				return right;
			}
			if (children[left] == p) {
				return left;
			}
			if (children[middle] == p) {
				return middle;
			}
			if (children[right] == p) {
				return right;
			}
			if (children[middle] < p) {
				left = middle + 1;
				middle = (left + right)  / 2;
				continue;
			}
			if (children[middle] > p) {
				right = middle - 1;
				middle = (left + right) / 2;
				continue;
			}
		}
	}

	/**
	 *
	 * @param key the key being searched
	 * @param keys the keys used for comparison
	 * @return the position of the child (-1 will be used for the left most child in the case of a branch node).
	 */
	private int determinePosition(final Object key, final List<BTreeEntry> entries) {

		// Get original positions
		int left = 0;
		int right = entries.size() - 1;
		int middle = right / 2;

		while (true) {
			if (key == null) {
				return 0;
			}

			if (entries.get(left) == null) {
				left++;
				continue;
			}

			// If left greater than or equal to right, return right
			if (left >= right) {
				return right;
			}

			// First things first so as not to waste time searching. Compare key to left.
			if (comparator.compare((K)key, (K)entries.get(left).getKey()) < 0) {
				return 0;
			}

			if (comparator.compare((K)key, (K)entries.get(left).getKey()) == 0) {
				return left;
			}

			// If they key is greater than right, then return the length of keys
			if ((K)entries.get(right).getKey() != null && comparator.compare((K)key, (K)entries.get(right).getKey()) > 0) {
				return entries.size();
			}

			if ((K)entries.get(right).getKey() != null && comparator.compare((K)key, (K)entries.get(right).getKey()) == 0) {
				return right;
			}

			// Check the middle key is not null, or if the key is less than or equal to middle
			if ((K)entries.get(middle).getKey() == null || comparator.compare((K)key, (K)entries.get(middle).getKey()) <= 0) {
				right = middle;
				middle = (left + right) / 2;
				// continue as not found
				continue;
			}

			// If they key is greater than middle, then set left to middle + 1
			// and create a new middle.
			if (comparator.compare((K)key, (K)(K)entries.get(middle).getKey()) > 0) {
				left = middle + 1;
				middle = (left + right) / 2;
				continue;
			}

		}

	}

	public V update(K key, V value) {
		return value;
	}

	private V insertEntry(final K key, final V v) throws IOException, DuplicateKeyException {
		return null;
	}

	private void updateNodeAncestors(long newNode, long[] ancestors) {

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

	private static final class Pointer {

		final long pointer;

		public Pointer(long pointer) {
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

	public static class BTreeNode {

		private final List<BTreeEntry> entries;
		private final boolean leaf;

		public BTreeNode(int maxNodeSize, boolean leaf) {
			entries = new ArrayList<>(maxNodeSize);
			this.leaf = leaf;
		}

		public List<BTreeEntry> getEntries() {
			return entries;
		}

		public boolean isLeaf() {
			return leaf;
		}
	}

	private static class BTreeEntry {
		private Object key;
		private Object value;
		private Pointer left;
		private Pointer right;

		public Object getKey() {
			return key;
		}

		public void setKey(Object key) {
			this.key = key;
		}

		public Object getValue() {
			return value;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public Pointer getLeft() {
			return left;
		}

		public void setLeft(Pointer left) {
			this.left = left;
		}

		public Pointer getRight() {
			return right;
		}

		public void setRight(Pointer right) {
			this.right = right;
		}
	}

//	private static final class BranchNode implements BTreeNode {
//		final Object[] keys;
//		final long[] children;
//
//		public BranchNode(Object[] keys, long[] children) {
//			this.keys = keys;
//			this.children = children;
//		}
//
//		public BranchNode(Object[] keys, List<Long> children) {
//			this.keys = keys;
//			this.children = new long[children.size()];
//			for (int i = 0; i < children.size(); i++) {
//				this.children[i] = children.get(i);
//			}
//		}
//
//		@Override
//		public Object[] keys() {
//			return keys;
//		}
//
//		@Override
//		public Object[] values() {
//			return null;
//		}
//
//		@Override
//		public long[] children() {
//			return children;
//		}
//
//		public long next() {
//			return children[children.length - 1];
//		}
//
//		@Override
//		public boolean isLeaf() {
//			return false;
//		}
//
//		@Override
//		public String toString() {
//			return "BranchNode{" +
//					"keys=" + Arrays.toString(keys) +
//					", children=" + Arrays.toString(children) +
//					'}';
//		}
//	}
//
//	private static final class LeafNode implements BTreeNode {
//		Object[] keys;
//		Object[] values;
//		final long next;
//
//		public LeafNode(Object[] keys, Object[] values, long next) {
//			this.keys = keys;
//			this.values = values;
//			this.next = next;
//		}
//
//		@Override
//		public Object[] keys() {
//			return keys;
//		}
//
//		@Override
//		public Object[] values() {
//			return values;
//		}
//
//		@Override
//		public long[] children() {
//			return null;
//		}
//
//		@Override
//		public long next() {
//			return next;
//		}
//
//		@Override
//		public boolean isLeaf() {
//			return true;
//		}
//	}

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
