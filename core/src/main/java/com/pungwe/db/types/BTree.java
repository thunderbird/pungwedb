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

	public V get(K key) throws IOException {
		if (key == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (rootPointer == null) {
			return null;
		}

		BTreeNode node = store.get(rootPointer.pointer, nodeSerializer);
		while (!node.isLeaf()) {
			int pos = determinePosition(key, node.getEntries());
			if (pos >= node.getEntries().size()) {
				// Go to the right and continue
				BTreeEntry entry = node.getEntries().get(node.getEntries().size() - 1);
				node = store.get(entry.getRight().pointer, nodeSerializer);
				continue;
			}

			BTreeEntry entry = node.getEntries().get(pos);
			// If pos is 0 and the key is less than pos, then go right
			if (pos == 0 && comparator.compare(key, (K)entry.getKey()) < 0) {
				node = store.get(entry.getLeft().pointer, nodeSerializer);
				continue;
			}
			node = store.get(entry.getRight().pointer, nodeSerializer);
		}

		int pos = determinePosition(key, node.getEntries());
		// we should always get the correct position.
		if (pos == node.getEntries().size()) {
			return null;
		}

		BTreeEntry entry = node.getEntries().get(pos);
		if (referencedValue) {
			return store.get(((Pointer)entry.getValue()).pointer, valueSerializer);
		}
		return (V)entry.getValue();
	}

	// TODO: Fix add, as it assumes unique and no duplicate dropping
    // It also assumes non-null keys
	public V add(K key, V value) throws IOException, DuplicateKeyException {
		if (key == null && value == null) {
			throw new IllegalArgumentException("Both key and value cannot be null");
		}
		if (value == null) {
			throw new IllegalArgumentException("Value cannot be null");
		}

		// Is the root node null?
		if (rootPointer == null) {
			BTreeNode root = new BTreeNode(maxNodeSize, true);
			BTreeEntry entry = new BTreeEntry();
			entry.setKey(key);
			entry.setValue(processValue(value));
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
        List<Long> pointers = new ArrayList<>(4);
		pointers.add(rootPointer.pointer);
		// Find the leaf node
		while (!node.isLeaf()) {
			int pos = determinePosition(key, node.getEntries());
			BTreeEntry entry = node.getEntries().get(pos);
			if (entry.getLeft() != null && comparator.compare(key, (K)entry.getKey()) < 0) {
                pointers.add(0, entry.getLeft().pointer);
				node = store.get(entry.getLeft().pointer, nodeSerializer);
			} else {
                pointers.add(0, entry.getRight().pointer);
				node = store.get(entry.getRight().pointer, nodeSerializer);
			}
			// Push to beginning of array list
			nodes.add(0, node);
		}

		// Find position to insert
		int pos = determinePosition(key, node.getEntries());

        // Add to the end
        if (pos == node.getEntries().size()) {
            BTreeEntry entry = new BTreeEntry();
            entry.setKey(key);
            entry.setValue(processValue(value));
            node.getEntries().add(entry);
            updateNodes(nodes, entry, pointers);
            return value;
        }

		BTreeEntry found = node.getEntries().get(pos);
		// If the key is the same, then check the unique value
		// TODO: Support dups
		int comp = comparator.compare(key, (K)found.getKey());
		if (comp == 0) {
			if (unique && !dropDups) {
				throw new DuplicateKeyException("Key: " + key + " is not unique");
			} else if (unique && dropDups) {
                // Reset the value
                found.setValue(processValue(value));
				updateNodes(nodes, found, pointers);
				return value;
			} else {
				throw new UnsupportedOperationException("Non-unique not supported yet");
			}
		} else {
            BTreeEntry entry = new BTreeEntry();
            entry.setKey(key);
            entry.setValue(processValue(value));
            node.getEntries().add(pos, entry);
            updateNodes(nodes, entry, pointers);
            return value;
        }
	}

    private Object processValue(V value) throws IOException {
        if (referencedValue) {
            long p = store.put(value, valueSerializer);
            return new Pointer(p);
        }
        return value;
    }

	// FIXME: Check for split here.
	private void updateNodes(List<BTreeNode> nodes, BTreeEntry entry, List<Long> pointers) throws IOException {
		// Store the first node in the list...
		long newPointer = store.put(nodes.get(0), nodeSerializer);
		long current = pointers.get(0);
		if (newPointer == current) {
			// no need to change anything here as we are updating directly and it hasn't moved...
			return;
		}
		// Iterate every node after that...
		for (int i = 1; i < nodes.size(); i++) {
			BTreeNode node = nodes.get(i); // parent node
            // Set the new pointer against the node
            if (entry.getLeft() != null && entry.getLeft().pointer == current) {
                entry.setLeft(new Pointer(newPointer));
            } else if (entry.getRight() != null && entry.getRight().pointer == current) {
                entry.setRight(new Pointer(newPointer));
            }
			newPointer = store.put(node, nodeSerializer);
			current = pointers.get(i);
			if (newPointer == current) {
				return;
			}
		}
	}

	/**
	 *
	 * @param key the key being searched
	 * @param entries the entries used for comparison
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
