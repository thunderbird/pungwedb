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
// FIXME: Remove valuse serialization, which is bad... They should be done separately
// TODO: Add node size increments
/**
 * Created by ian on 03/10/2014.
 */
public class BTree<K,V> implements Iterable<BTree<K,V>> {

	private static final Logger log = LoggerFactory.getLogger(BTree.class);

	private final ConcurrentMap<Long, Thread> locks = new ConcurrentHashMap<>();

	private final Store store;
	private final Serializer<K> keySerializer;
	private final Serializer<V> valueSerializer;
	private final Serializer<BTreeNode> nodeSerializer;

	private final Comparator<K> comparator;

	private final boolean unique;
	private final int maxNodeSize;
	private final boolean referencedValue;


	protected Pointer rootPointer;

	// Used for creating a BTree
	public BTree(Store store, Comparator<K> comparator, Serializer<K> keySerializer, Serializer<V> valueSerializer,
				 boolean unique, int maxNodeSize, boolean referencedValue) throws IOException {
		this(store, -1, comparator, keySerializer, valueSerializer, unique, maxNodeSize, referencedValue);
	}

	public BTree(Store store, long pointer, Comparator<K> comparator, Serializer<K> keySerializer, Serializer<V> valueSerializer,
				 boolean unique, int maxNodeSize, boolean referencedValue) throws IOException {
		this.unique = unique;
		this.comparator = comparator;
		this.store = store;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.nodeSerializer = new BTreeNodeSerializer();
		this.referencedValue = referencedValue;
		this.maxNodeSize = maxNodeSize;

		// This can be null and can change...
		if (pointer != -1) {
			this.rootPointer = new Pointer(pointer);
		} else {
			BTreeNode root = new BTreeNode(maxNodeSize, true);
			long p = store.put(root, nodeSerializer);
			rootPointer = new Pointer(p);
		}
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
			if (pos < 0) {
				pos = 0;
			}
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
			// Left
			} else if (pos > 0 && comparator.compare(key, (K)entry.getKey()) < 0) {
				node = store.get(node.getEntries().get(pos - 1).getRight().pointer, nodeSerializer);
				continue;
			}
			node = store.get(entry.getRight().pointer, nodeSerializer);
		}

		int pos = determinePosition(key, node.getEntries());
		// we should always get the correct position.
		if (pos == node.getEntries().size() || pos < 0) {
			return null;
		}

		BTreeEntry entry = node.getEntries().get(pos);
		// FIXME: We need to handle referenced values that are not necessarily pointers
		if (entry.getValue() instanceof Pointer) {
			return store.get(((Pointer)entry.getValue()).pointer, valueSerializer);
		}
		return (V)entry.getValue();
	}

	public V add(K key, V value) throws IOException, DuplicateKeyException {
		return add(key, value, false);
	}

	// TODO: Fix add, as it assumes unique and no duplicate dropping
    // It also assumes non-null keys
	public V add(K key, V value, boolean replace) throws IOException, DuplicateKeyException {
		if (key == null && value == null) {
			throw new IllegalArgumentException("Both key and value cannot be null");
		}
		if (value == null) {
			throw new IllegalArgumentException("Value cannot be null");
		}

		// Root exists
		BTreeNode node = store.get(rootPointer.getPointer(), nodeSerializer);
		// Always add root to the stack
		List<BTreeNode> nodes = new ArrayList<>(4);
		nodes.add(node);
        List<Long> pointers = new ArrayList<>(4);
		pointers.add(rootPointer.pointer);
		// Find the leaf node
		while (!node.isLeaf()) {
			int pos = determinePosition(key, node.getEntries());
			if (pos < 0) {
				pos = 0;
			} else if (pos >= node.getEntries().size()) {
				pos = node.getEntries().size() - 1;
			}
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
            entry.setValue(processValue(-1, value));
            node.getEntries().add(entry);
            updateNodes(nodes, key, pointers);
            return value;
        }

		// Not found, add to beginning
		if (pos < 0) {
			BTreeEntry entry = new BTreeEntry();
            entry.setKey(key);
            entry.setValue(processValue(-1, value));
            node.getEntries().add(0, entry);
            updateNodes(nodes, key, pointers);
            return value;
		}

		BTreeEntry found = node.getEntries().get(pos);
		int comp = comparator.compare(key, (K)found.getKey());
		if (comp == 0) {
			if (!replace) {
				throw new DuplicateKeyException("Key: " + key + " is not unique");
			} else {
                // Reset the value
				// FIXME: This needs to handle secondary indexes properly.
				if (referencedValue) {
					Pointer p = (Pointer)found.getValue();
					store.remove(p.pointer);
				}
                found.setValue(processValue(-1, value));
				updateNodes(nodes, key, pointers);
				return value;
			}
		} else {
            BTreeEntry entry = new BTreeEntry();
            entry.setKey(key);
            entry.setValue(processValue(-1, value));
            node.getEntries().add(pos, entry);
            updateNodes(nodes, key, pointers);
            return value;
        }
	}

    private Object processValue(long pos, V value) throws IOException {
        if (referencedValue && pos == -1) {
            long p = store.put(value, valueSerializer);
            return new Pointer(p);
        } else if (referencedValue) {
			long p = store.update(pos, value, valueSerializer);
			return new Pointer(p);
		}
        return value;
    }

	// FIXME: Check for split here.
	private void updateNodes(List<BTreeNode> nodes, K key, List<Long> pointers) throws IOException {
		// Check for split
		if (nodes.get(0).getEntries().size() > maxNodeSize) {
			split(nodes, pointers);
			return;
		}

		long newPointer = -1, previous = -1;
		for (int i = 0; i < nodes.size(); i++) {
			BTreeNode node = nodes.get(i);
			long current = pointers.get(i);
			// If the node is a leaf, just store it.
			if (node.isLeaf()) {
				newPointer = store.update(current, node, nodeSerializer);
				if (current == newPointer) {
					return;
				} else if (current == rootPointer.getPointer()) {
					rootPointer = new Pointer(newPointer);
					return;
				}
				previous = current;
			} else {
				// New pointer will not be -1. The fact that we are here is because the pointer of the leaf changed
				int pos = determinePosition(key, node.getEntries());
				if (pos < 0) {
					pos = 0;
				} else if (pos >= node.getEntries().size()) {
					pos = node.getEntries().size() - 1;
				}
				BTreeEntry entry = node.getEntries().get(pos);
				if (entry.getLeft() != null && entry.getLeft().pointer == previous) {
					entry.setLeft(new Pointer(newPointer));
				} else {
					entry.setRight(new Pointer(newPointer));
				}
				newPointer = store.update(current, node, nodeSerializer);
				if (current == newPointer) {
					return;
				} else if (current == rootPointer.getPointer()) {
					rootPointer = new Pointer(newPointer);
					return;
				}
				previous = current;
			}
		}
	}

	private void split(List<BTreeNode> nodes, List<Long> pointers) throws IOException {
		// First node will need splitting as it's the leaf...
		int current = 0;
		BTreeNode node = null;
		while (true) {
			node = nodes.get(current);

			int size = node.getEntries().size();
			if (size < maxNodeSize) {
				return; // do nothing
			}
			int medianIndex = (size - 1)  >>> 1;

			// Get Left entries
			List<BTreeEntry> leftEntries = new ArrayList<>(size / 2);
			for (int i = 0; i < medianIndex; i++) {
				leftEntries.add(node.getEntries().get(i));
			}

			// Get right entries
			List<BTreeEntry> rightEntries = new ArrayList<>(size / 2);
			for (int i = medianIndex; i < size; i++) {
				rightEntries.add(node.getEntries().get(i));
			}

			// Get the middle value
			BTreeEntry medianEntry = node.getEntries().get(medianIndex);

			// No parent... So we need to create a new root and save it's two child nodes.
			if (current + 1 >= nodes.size()) {
				BTreeNode newRoot = new BTreeNode(maxNodeSize, false);
				BTreeEntry entry = new BTreeEntry();
				entry.setKey(medianEntry.getKey());

				// Create the left node
				BTreeNode left = new BTreeNode(maxNodeSize, node.isLeaf());
				left.getEntries().addAll(leftEntries);
				long lp = store.put(left, nodeSerializer);

				// Create the right node
				BTreeNode right = new BTreeNode(maxNodeSize, node.isLeaf());
				right.getEntries().addAll(rightEntries);
				long rp = store.put(right, nodeSerializer);

				// Set left and right on the entry
				entry.setLeft(new Pointer(lp));
				entry.setRight(new Pointer(rp));

				// Add the entry to the new root node
				newRoot.getEntries().add(entry);

				long p = store.put(newRoot, nodeSerializer);
				rootPointer = new Pointer(p);
				// Remove the old node
				store.remove(pointers.get(current));
				return; // nothing else to do.
			}

			// There is a parent node, so we need to get that and add the relevant keys to it
			BTreeNode parent = nodes.get(current + 1);
			int pos = determinePosition(medianEntry.getKey(), parent.getEntries());

			BTreeNode rightNode = new BTreeNode(maxNodeSize, node.isLeaf());
			rightNode.getEntries().addAll(rightEntries);

			// Create a new left hand node
			BTreeNode leftNode = new BTreeNode(maxNodeSize, node.isLeaf());
			leftNode.setEntries(leftEntries);

			BTreeEntry parentEntry = new BTreeEntry();
			parentEntry.setKey(medianEntry.getKey());
			// Store the new left hand node
			long lp = store.put(leftNode, nodeSerializer);

			if (pos <= 0) {
				BTreeEntry oldFirst = parent.getEntries().get(0);
				if (oldFirst.getLeft() != null) {
					BTreeNode oldLeft = store.get(oldFirst.getLeft().pointer, nodeSerializer);
					rightNode.getEntries().addAll(oldLeft.getEntries());
					// Remove the old left hand node...
					store.remove(oldFirst.getLeft().pointer);
				}
				parentEntry.setLeft(new Pointer(lp));
			} else {
				BTreeEntry leftEntry = parent.getEntries().get(pos - 1);
				leftEntry.setRight(new Pointer(lp));
			}
			// Save the right hand node
			long rp = store.put(rightNode, nodeSerializer);

			// Set the newly saved right pointer.
			parentEntry.setRight(new Pointer(rp));
			parent.getEntries().add(pos, parentEntry);

			// Update the parent node
			long oldPointer = pointers.get(current + 1);
			long newPointer = store.update(pointers.get(current + 1), parent, nodeSerializer);
			if (oldPointer != newPointer) {
				pointers.set(current + 1, newPointer);
			}

			if (oldPointer == rootPointer.pointer && newPointer != oldPointer) {
				System.out.println("Root Pointer changed during split");
				rootPointer = new Pointer(newPointer);
			}

			// Remove the old node.
			store.remove(pointers.get(current));
			current++;
		}

	}

	/**
	 *
	 * @param key the key being searched
	 * @param entries the entries used for comparison
	 * @return the position of the child (-1 will be used for the left most child in the case of a branch node).
	 */
	private int determinePosition(final Object key, final List<BTreeEntry> entries) {
		int low = 0;
		int high = entries.size()-1;

		while (true) {
			if (key == null) {
				return 0;
			}
			int mid = (low + high) >>> 1;
			if (low > high) {
				return high;
			}
			if (entries.get(low) == null) {
				low++;
			}
			if (comparator.compare((K)key, (K)entries.get(low).getKey()) < 0) {
				return low == 0 ? low : low - 1;
			}
			if (comparator.compare((K)key, (K)entries.get(low).getKey()) == 0) {
				return low;
			}
			if (comparator.compare((K)key, (K)entries.get(low).getKey()) > 0) {
				low++;
			}
			if (comparator.compare((K)key, (K)entries.get(mid).getKey()) < 0) {
				high = mid - 1;
			}
			if (comparator.compare((K)key, (K)entries.get(mid).getKey()) == 0) {
				return mid;
			}
			if (comparator.compare((K)key, (K)entries.get(mid).getKey()) > 0) {
				low = mid + 1;
			}
			if (comparator.compare((K)key, (K)entries.get(high).getKey()) < 0) {
				high--;
			}
			if (comparator.compare((K)key, (K)entries.get(high).getKey()) == 0) {
				return high;
			}
			if (comparator.compare((K)key, (K)entries.get(high).getKey()) > 0) {
				return high + 1;
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

	public void unlockAll() {
		// do nothing...
		Thread t = Thread.currentThread();
	}

	public void unlock(Long v) {
		final Thread t = locks.remove(v);
	}

	protected static final class Pointer {

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

		public long getPointer() {
			return pointer;
		}

		@Override
		public String toString() {
			return "BTree Reference{" +
					"pointer=" + pointer +
					'}';
		}
	}

	public static class BTreeNode {

		private List<BTreeEntry> entries;
		private final boolean leaf;
		private final int maxNodeSize;

		public BTreeNode(int maxNodeSize, boolean leaf) {
			this.entries = new ArrayList<>(maxNodeSize);
			this.leaf = leaf;
			this.maxNodeSize = maxNodeSize;
		}

		public int getMaxNodeSize() {
			return maxNodeSize;
		}

		public List<BTreeEntry> getEntries() {
			return entries;
		}

		public boolean isLeaf() {
			return leaf;
		}

		public void setEntries(List<BTreeEntry> entries) {
			this.entries = entries;
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
			// This should be a pretty straight forward task
			out.writeByte(TypeReference.INDEX.getType());
			out.writeInt(value.getMaxNodeSize());
			out.writeInt(value.getEntries().size()); // write the number of entries
			out.writeBoolean(value.isLeaf()); // leaf node or not?
			Iterator<BTreeEntry> it = value.getEntries().iterator();
			while (it.hasNext()) {
				BTreeEntry entry = it.next();
				out.writeByte(TypeReference.ENTRY.getType());
				if (!value.isLeaf()) {
					// Write Child nodes
					out.writeLong(entry.getLeft() == null ? -1 : entry.getLeft().pointer);
					out.writeLong(entry.getRight() == null ? -1 : entry.getRight().pointer);
				}
				// Record the key type
				out.writeByte(TypeReference.forClass(entry.getKey() == null ? null : entry.getKey().getClass()).getType());
				// Write the key
				keySerializer.serialize(out, (K)entry.getKey());
				// We need to determine the type of object being stored
				if (value.isLeaf()) {
					if (entry.getValue() instanceof Pointer) {
						out.writeByte(TypeReference.POINTER.getType());
						out.writeLong(((Pointer) entry.getValue()).pointer);
					} else {
						out.writeByte(TypeReference.forClass(entry.getValue() == null ? null : entry.getValue().getClass()).getType());
						if (entry.getValue() != null) {
							valueSerializer.serialize(out, (V)entry.getValue());
						}
					}
				}
			}
		}

		@Override
		public BTreeNode deserialize(DataInput in) throws IOException {
			byte t = in.readByte();
			assert t == TypeReference.INDEX.getType() : "Wrong type: " + TypeReference.fromType(t);
			int maxNodeSize = in.readInt();
			int entries = in.readInt();
			boolean leaf = in.readBoolean();
			BTreeNode node = new BTreeNode(maxNodeSize, leaf);
			for (int i = 0; i < entries; i++) {

				byte e = in.readByte(); // Type Reference
				assert TypeReference.fromType(e) == TypeReference.ENTRY : "Not an entry";

				BTreeEntry entry = new BTreeEntry();

				// If this node is a branch, it possibly has a left and right child
				if (!node.isLeaf()) {
					long lp = in.readLong();
					long rp = in.readLong();
					entry.setLeft(lp != -1 ? new Pointer(lp) : null);
					entry.setRight(rp != -1 ? new Pointer(rp) : null);
				}

				// Get the key type
				byte kt = in.readByte();
				Object key = keySerializer.deserialize(in);
				entry.setKey(key);

				// Get the value type if it's a leaf
				if (node.isLeaf()) {
					byte vt = in.readByte();
					Object value = null;
					if (vt == TypeReference.POINTER.getType()) {
						value = new Pointer(in.readLong());
					} else if (vt != TypeReference.NULL.getType()) {
						value = valueSerializer.deserialize(in);
					}
					entry.setValue(value);
				}

				// Add the entry to the node.
				node.getEntries().add(entry);
			}
			return node;
		}

		@Override
		public TypeReference getTypeReference() {
			return null;
		}
	}
}
