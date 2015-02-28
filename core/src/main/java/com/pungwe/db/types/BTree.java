package com.pungwe.db.types;

import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.exception.DuplicateKeyException;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.store.Store;
import org.apache.commons.collections4.map.LRUMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantReadWriteLock;

// FIXME: Add modification listener

/**
 * Created by 917903 on 12/02/2015.
 */
public class BTree<K, V> {

	private static final Logger log = LoggerFactory.getLogger(BTree.class);
	protected final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

	protected final Store store;
	protected final Serializer<K> keySerializer;
	protected final Serializer<V> valueSerializer;
	protected final Serializer<BTreeNode> nodeSerializer = new BTreeNodeSerializer();
	protected final Comparator<K> comparator;
	protected final boolean referencedValue;
	protected final int maxNodeSize;

	protected Pointer rootPointer;

	public BTree(Store store, Comparator<K> comparator, Serializer<K> keySerializer, Serializer<V> valueSerializer, int maxNodeSize, boolean referencedValue) throws IOException {
		this(store, -1, comparator, keySerializer, valueSerializer, maxNodeSize, referencedValue);
	}

	public BTree(Store store, final long pointer, Comparator<K> comparator, Serializer<K> keySerializer, Serializer<V> valueSerializer, int maxNodeSize, boolean referencedValue) throws IOException {
		this.store = store;
		this.comparator = comparator;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.referencedValue = referencedValue;
		this.maxNodeSize = maxNodeSize;

		// Stops over serialization
		//this.keyCache = new LRUMap<>(maxNodeSize + 1); // always room for an extra key

		// This can be null and can change...
		this.rootPointer = new Pointer(pointer);
		if (rootPointer.getPointer() == -1) {
			BTreeNode root = new BTreeNode(maxNodeSize, true);
			saveNode(rootPointer, root);
		}
	}

	public V add(K key, V value) throws IOException, DuplicateKeyException {
		return add(key, value, false);
	}

	public V add(K key, V value, boolean replace) throws IOException, DuplicateKeyException {

		try {

			readWriteLock.writeLock().lock();

			if (key == null && value == null) {
				throw new IllegalArgumentException("Both key and value cannot be null");
			}
			if (value == null) {
				throw new IllegalArgumentException("Value cannot be null");
			}

			List<Pointer> pointers = new ArrayList<>(4);

			BTreeNode node = getNode(rootPointer);
			Pointer current = rootPointer;

			// Find the leaf node
			while (!node.isLeaf()) {

				Pointer pointer = null;

				pointers.add(0, current);

				int pos = determinePosition(key, node.getKeys());

				// Straight forward, if key is less than 0 it's the first child. If not, it's either pos or pos + 1
				if (pos < 0) {
					pointer = (Pointer) node.getChildren().get(0);
					// If pos is the same as the key size, then take right most key... If it's less than the key at pos; then we go left
				} else if (pos == node.getKeys().size() || comparator.compare(key, (K) processKey(node.getKeys().get(pos))) < 0) {
					pointer = (Pointer) node.getChildren().get(pos);
				} else {
					pointer = (Pointer) node.getChildren().get(pos + 1);
				}
				node = getNode(pointer);

				current = pointer;

			}

			// Find position to insert
			int pos = determinePosition(key, node.getKeys());

			// Add to the end
			if (pos >= node.getKeys().size()) {
				node.getKeys().add(processNewKey(key));
				node.getValues().add(processValue(new Pointer(-1), value));
			} else if (pos < 0) {
				// Not found, add to beginning
				node.getKeys().add(0, processNewKey(key));
				node.getValues().add(0, processValue(new Pointer(-1), value));
			} else {

				Object found = processKey(node.getKeys().get(pos));
				Object oldValue = node.getValues().get(pos);
				int comp = comparator.compare(key, (K) found);
				if (comp == 0) {
					if (!replace) {
						throw new DuplicateKeyException("Key: " + key + " is not unique");
					} else {
						// Reset the value
						// FIXME: This needs to handle secondary indexes properly.
						if (referencedValue) {
							Pointer p = (Pointer) oldValue;
							removeNode(p);
						}
						node.getValues().set(pos, processValue(new Pointer(-1), value));
					}
				} else if (comp > 0) {
					node.getKeys().add(pos + 1, processNewKey(key));
					node.getValues().add(pos + 1, processValue(new Pointer(-1), value));
				} else {
					node.getKeys().add(pos, processNewKey(key));
					node.getValues().add(pos, processValue(new Pointer(-1), value));
				}
			}

			split(node, key, current, pointers);

			return value;
		} finally {
			if (readWriteLock.writeLock().isHeldByCurrentThread()) {
				readWriteLock.writeLock().unlock();
			}
		}
	}

	public V get(K key) throws IOException {

		try {
			readWriteLock.readLock().lock();

			if (key == null) {
				throw new IllegalArgumentException("Key cannot be null");
			}
			if (rootPointer == null) {
				return null;
			}

			Pointer current = rootPointer;

			BTreeNode node = getNode(current);
			while (!node.isLeaf()) {
				Pointer previous = current;
				int pos = determinePosition(key, node.getKeys());
				// Straight forward, if key is less than 0 it's the first child. If not, it's either pos or pos + 1
				if (pos < 0) {
					Pointer pointer = (Pointer) node.getChildren().get(0);
					node = getNode(pointer);
					current = pointer;
				} else if (pos == node.getKeys().size() || comparator.compare(key, (K) processKey(node.getKeys().get(pos))) < 0) {
					// Always shift right
					Pointer pointer = (Pointer) node.getChildren().get(pos);
					current = pointer;
					node = getNode(pointer);
				} else {
					// Always shift right
					Pointer pointer = (Pointer) node.getChildren().get(pos + 1);
					node = getNode(pointer);
					current = pointer;
				}
			}

			int pos = determinePosition(key, node.getKeys());
			// we should always get the correct position.
			if (pos == node.getKeys().size() || pos < 0) {
				return null;
			}

			if (comparator.compare((K) key, (K) processKey(node.getKeys().get(pos))) != 0) {
				// Key not found
				return null;
			}

			// Process the value retrieved from the node...
			return (V) getValue(node.getValues().get(pos));
		} finally {
			readWriteLock.readLock().unlock();
		}
	}

	// FIXME: Add this
	public V remove(K key) throws IOException {
		return null;
	}

	public V update(K key, V value) throws IOException, DuplicateKeyException {
		return add(key, value, true);
	}

	private void updateNodes(BTreeNode node, K key, Pointer pointer, List<Pointer> pointers) throws IOException {
		saveNode(pointer, node);
	}

	private void split(final BTreeNode node, K key, Pointer pointer, List<Pointer> pointers) throws IOException {

		int size = node.getKeys().size();

		// we shouldn't end up here...
		if (size <= maxNodeSize) {
			updateNodes(node, key, pointer, pointers);
			return;
		}

		// Get the middle (this should be in order).
		int medianIndex = (size - 1) >>> 1;

		// populate left node
		BTreeNode left = new BTreeNode(maxNodeSize, node.isLeaf());
		for (int i = 0; i < medianIndex; i++) {
			left.getKeys().add(node.getKeys().get(i));
		}
		if (!node.isLeaf()) {
			for (int i = 0; i < medianIndex + 1; i++) {
				left.getChildren().add(node.getChildren().get(i));
			}
		} else {
			for (int i = 0; i < medianIndex; i++) {
				left.getValues().add(node.getValues().get(i));
			}
		}
		// populate right node
		BTreeNode right = new BTreeNode(maxNodeSize, node.isLeaf());
		for (int i = (node.isLeaf() ? medianIndex : medianIndex + 1); i < size; i++) {
			right.getKeys().add(node.getKeys().get(i));
		}
		if (!node.isLeaf()) {
			for (int i = medianIndex + 1; i < node.getChildren().size(); i++) {
				right.getChildren().add(node.getChildren().get(i));
			}
		} else {
			for (int i = medianIndex; i < node.getValues().size(); i++) {
				right.getValues().add(node.getValues().get(i));
			}
		}

		Pointer leftPointer = new Pointer(-1);
		Pointer rightPointer = new Pointer(-1);

		saveNode(leftPointer, left);
		saveNode(rightPointer, right);

		assert leftPointer.getPointer() != -1 && rightPointer.getPointer() != -1;
		// New root from leaf
		if (pointers.size() == 0) {
			// left
			BTreeNode newRoot = new BTreeNode(maxNodeSize, false);
			// Add the initial root key
			newRoot.getKeys().add(node.getKeys().get(medianIndex));
			newRoot.getChildren().add(leftPointer);
			newRoot.getChildren().add(rightPointer);
			removeNode(pointer);
			saveNode(rootPointer, newRoot);
			return;
		}

		// Median key
		Object medianKey = node.getKeys().get(medianIndex);
		// New parent
		Pointer parentPointer = pointers.get(0);
		BTreeNode parent = getNode(parentPointer);
		// Find where it lives in the parent
		int pos = determinePosition(processKey(medianKey), parent.getKeys());
		// if the position is less than 0, then we need to add the key to the beginning
		// and add the left node to the left and the old left node to the right
		if (pos < 0) {
			parent.getKeys().add(0, medianKey);
			parent.getChildren().add(0, leftPointer);
			parent.getChildren().set(1, rightPointer);
		} else if (pos == parent.getKeys().size()) {
			parent.getKeys().add(medianKey);
			// Change old right to new left
			parent.getChildren().set(pos, leftPointer);
			parent.getChildren().add(rightPointer);
		} else {
			Object oldKey = parent.getKeys().get(pos);
			int comp = comparator.compare((K) processKey(medianKey), (K) processKey(oldKey));
			assert comp != 0 : "Should never happen";
			if (comp > 0) {
				parent.getKeys().add(pos + 1, medianKey);
				parent.getChildren().set(pos + 1, leftPointer);
				parent.getChildren().add(pos + 2, rightPointer);
			} else {
				parent.getKeys().add(pos, medianKey);
				parent.getChildren().set(pos, leftPointer);
				parent.getChildren().add(pos + 1, rightPointer);
			}
		}
		// Remove the old pointer as we don't need it anymore
		removeNode(pointer);

		split(parent, (K)medianKey, parentPointer, pointers.subList(1, pointers.size()));

	}

//	public void lock(Pointer v) {
//		final Thread t = Thread.currentThread();
//		final Thread locked = locks.get(v);
//		if (locked != null && locked.getId() == t.getId()) {
//			System.out.println("Already locked: " + t.getId());
//			return;
//		}
//
//		Thread locker = null;
//		// Attempt to get the lock. If it's not available wait 10 nanoseconds...
//		while ((locker = locks.putIfAbsent(v, t)) != null) {
//			LockSupport.parkNanos(10);
//		}
//	}
//
//	public void unlockAll() {
//		final Thread t = Thread.currentThread();
//		Iterator<Map.Entry<Pointer, Thread>> it = locks.entrySet().iterator();
//		while (it.hasNext()) {
//			Map.Entry<Pointer, Thread> e = it.next();
//			if (e.getValue() == t) {
//				it.remove();
//			}
//		}
//
//	}
//
//	public void unlock(Pointer v) {
//		final Thread t = locks.remove(v);
//	}

	protected BTreeNode getNode(Pointer p) throws IOException {
//		synchronized (nodeCache) {
//			BTreeNode n = nodeCache.get(p);
//			if (n != null) {
//				return n;
//			}
//			n = store.get(p.getPointer(), nodeSerializer);
//			nodeCache.put(p, n);
//			return n;
//		}
		return store.get(p.getPointer(), nodeSerializer);
	}

	protected void saveNode(Pointer pointer, BTreeNode n) throws IOException {

//		synchronized (nodeCache) {
		if (pointer.getPointer() == -1) {
			pointer.setPointer(store.put(n, nodeSerializer));
		} else {
			final long p = pointer.getPointer();
			pointer.setPointer(store.update(p, n, nodeSerializer));
		}
//			nodeCache.putIfAbsent(pointer, n);
//		}

	}

	protected void removeNode(Pointer p) throws IOException {
//		synchronized (nodeCache) {
//			nodeCache.remove(p);
		store.remove(p.getPointer());
//		}
	}

	protected Object getValue(Object v) throws IOException {
		if (v instanceof Pointer && referencedValue) {
			return store.get(((Pointer) v).getPointer(), valueSerializer);
		}
		return v;
	}

	private Object processValue(Pointer pos, V value) throws IOException {
		if (referencedValue && pos.getPointer() == -1) {
			long p = store.put(value, valueSerializer);
			pos.setPointer(p);
			return pos;
		} else if (referencedValue) {
			long p = store.update(pos.getPointer(), value, valueSerializer);
			pos.setPointer(p);
		}
		return value;
	}

	private Object processNewKey(Object key) throws IOException {
		TypeReference kt = TypeReference.forClass(key == null ? null : key.getClass());
		// Write the key
		switch (kt) {
			case NULL:
			case BOOLEAN:
			case NUMBER:
			case POINTER:
			case DECIMAL: {
				return key;
			}
			default: {
				// Write reference key
				long kp = store.put((K) key, keySerializer);
				return new Pointer(kp);
			}
		}
	}

	private Object processKey(Object key) throws IOException {
//		if (key instanceof Pointer) {
//			if (!keyCache.containsKey((Pointer) key)) {
//				Object k = store.get(((Pointer) key).getPointer(), keySerializer);
//				keyCache.putIfAbsent((Pointer) key, key);
//				return k;
//			}
//			return keyCache.get((Pointer) key);
		if (key instanceof Pointer) {
			return store.get(((Pointer) key).getPointer(), keySerializer);
		} else {
			return key;
		}
	}

	/**
	 * @param key     the key being searched
	 * @param entries the entries used for comparison
	 * @return the position of the child (-1 will be used for the left most child in the case of a branch node).
	 */
	private int determinePosition(final Object key, final List<Object> entries) throws IOException {
		int low = 0;
		int high = entries.size() - 1;

		while (true) {

			int mid = (low + high) >>> 1;
			if (low > high) {
				return high;
			}

			Object lowKey = processKey((K) entries.get(low));

			if (lowKey == null && key != null) {
				low++;
			} else if (lowKey == null && key == null) {
				return low;
			}

			// Check the low
			if (comparator.compare((K) key, (K) lowKey) < 0) {
				return low - 1;
			}
			if (comparator.compare((K) key, (K) lowKey) == 0) {
				return low;
			}
			if (comparator.compare((K) key, (K) lowKey) > 0) {
				low++;
			}

			Object midKey = processKey((K) entries.get(mid));
			if (comparator.compare((K) key, (K) midKey) < 0) {
				high = mid - 1;
			}
			if (comparator.compare((K) key, (K) midKey) == 0) {
				return mid;
			}
			if (comparator.compare((K) key, (K) midKey) > 0) {
				low = mid + 1;
			}

			Object highKey = processKey((K) entries.get(high));
			if (comparator.compare((K) key, (K) highKey) < 0) {
				high--;
			}
			if (comparator.compare((K) key, (K) highKey) == 0) {
				return high;
			}
			if (comparator.compare((K) key, (K) highKey) > 0) {
				return high + 1;
			}
		}
	}

	private static final class BTreeNode {
		private List<Object> keys;
		private List<Object> values;
		private List<Object> children;
		private final boolean leaf;
		private final int maxNodeSize;

		public BTreeNode(int maxNodeSize, boolean leaf) {
			this.keys = new ArrayList<>(maxNodeSize);
			if (leaf) {
				this.values = new ArrayList<>(maxNodeSize); // First value is always left if leaf is false
			} else {
				this.children = new ArrayList<>(maxNodeSize + 1);
			}
			this.leaf = leaf;
			this.maxNodeSize = maxNodeSize;
		}

		public int getMaxNodeSize() {
			return maxNodeSize;
		}

		public List<Object> getKeys() {
			return keys;
		}

		public List<Object> getValues() {
			if (leaf) {
				return this.values;
			}
			throw new IllegalArgumentException("Cannot set values on a branch node");
		}

		public List<Object> getChildren() {
			if (!leaf) {
				return this.children;
			}
			throw new IllegalArgumentException("Cannot get children on a leaf node");
		}

		public boolean isLeaf() {
			return leaf;
		}
	}

	private final class BTreeNodeSerializer implements Serializer<BTreeNode> {

		@Override
		public void serialize(DataOutput out, BTreeNode value) throws IOException {


			// This should be a pretty straight forward task
			out.writeInt(value.getMaxNodeSize());
			out.writeBoolean(value.isLeaf()); // leaf node or not?
			out.writeInt(value.getKeys().size()); // write the number of entries

			// Write Keys
			Iterator<Object> it = value.getKeys().iterator();
			while (it.hasNext()) {
				Object key = it.next();
				TypeReference kt = TypeReference.POINTER;
				if (!(key instanceof Pointer)) {
					kt = TypeReference.forClass(key == null ? null : key.getClass());
				}
				assert kt != null : "Null type";
				// Write the key
				switch (kt) {
					case BOOLEAN: {
						out.writeByte(kt.getType());
						keySerializer.serialize(out, (K) key);
						break;
					}
					case NUMBER:
					case DECIMAL: {
						out.writeByte(kt.getType());
						keySerializer.serialize(out, (K) key);
						break;
					}
					case NULL: {
						out.writeByte(TypeReference.NULL.getType());
						break;
					}
					case POINTER: {
						out.writeByte(TypeReference.POINTER.getType());
						Pointer pointer = ((Pointer) key);
						out.writeLong(pointer.getPointer());
					}
				}
			}

			// Write child / value length
			if (value.isLeaf()) {
				out.writeInt(value.getValues().size());
			} else {
				out.writeInt(value.getChildren().size());
			}

			// Value iterator
			it = value.isLeaf() ? value.getValues().iterator() : value.getChildren().iterator();
			while (it.hasNext()) {
				Object key = it.next();
				TypeReference kt = TypeReference.POINTER;
				if (!(key instanceof Pointer)) {
					kt = TypeReference.forClass(key == null ? null : key.getClass());
				}
				// Write the key
				switch (kt) {
					case BOOLEAN: {
						out.writeByte(TypeReference.BOOLEAN.getType());
						valueSerializer.serialize(out, (V) key);
						break;
					}
					case NUMBER: {
						out.writeByte(TypeReference.NUMBER.getType());
						valueSerializer.serialize(out, (V) key);
						break;
					}
					case DECIMAL: {
						out.writeByte(TypeReference.DECIMAL.getType());
						valueSerializer.serialize(out, (V) key);
						break;
					}
					case NULL: {
						out.writeByte(TypeReference.NULL.getType());
						break;
					}
					case POINTER: {
						out.writeByte(TypeReference.POINTER.getType());
						Pointer pointer = ((Pointer) key);
						out.writeLong(pointer.getPointer());
					}
				}
			}
		}

		@Override
		public BTreeNode deserialize(DataInput in) throws IOException {

			int maxNodeSize = in.readInt();
			boolean leaf = in.readBoolean();
			int keySize = in.readInt();

			BTreeNode node = new BTreeNode(maxNodeSize, leaf);
			for (int i = 0; i < keySize; i++) {
				byte kt = in.readByte();
				TypeReference t = TypeReference.fromType(kt);
				switch (t) {
					case BOOLEAN:
					case NUMBER:
					case DECIMAL:
					case NULL: {
						Object key = keySerializer.deserialize(in);
						node.getKeys().add(key);
						break;
					}
					default: {
						long p = in.readLong(); // get the pointer
						node.getKeys().add(new Pointer(p));
					}
				}
			}

			int valueSize = in.readInt();
			for (int i = 0; i < valueSize; i++) {
				byte kt = in.readByte();
				TypeReference t = TypeReference.fromType(kt);
				assert t != null : "Cannot determine type";
				switch (t) {
					case BOOLEAN:
					case NUMBER:
					case DECIMAL:
					case NULL: {
						assert node.isLeaf() : "Node is not a leaf node, so invalid";
						Object key = valueSerializer.deserialize(in);
						node.getValues().add(key);
						break;

					}
					default: {
						long p = in.readLong(); // get the pointer
						if (node.isLeaf()) {
							node.getValues().add(new Pointer(p));
						} else {
							node.getChildren().add(new Pointer(p));
						}
					}
				}
			}
			return node;
		}

		@Override
		public TypeReference getTypeReference() {
			return TypeReference.OBJECT;
		}
	}
}
