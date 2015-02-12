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
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.locks.LockSupport;

// FIXME: Add modification listener

/**
 * Created by 917903 on 12/02/2015.
 */
public class BTreeMap<K, V> {

	private static final Logger log = LoggerFactory.getLogger(BTree.class);

	private final ConcurrentMap<Long, Thread> locks = new ConcurrentHashMap<>();

	protected final Store store;
	protected final Serializer<K> keySerializer;
	protected final Serializer<V> valueSerializer;
	protected final static Serializer<BTreeNode> nodeSerializer = new BTreeNodeSerializer();
	protected final Comparator<K> comparator;
	protected final boolean referencedValue;
	protected final LRUMap<Long, BTreeNode> nodeCache = new LRUMap<>(1000); // 1000 entry cache...
	protected final LRUMap<Pointer, Object> keyCache;
	protected final int maxNodeSize;

	protected Pointer rootPointer;

	public BTreeMap(Store store, Comparator<K> comparator, Serializer<K> keySerializer, Serializer<V> valueSerializer, int maxNodeSize, boolean referencedValue) throws IOException {
		this(store, -1, comparator, keySerializer, valueSerializer, maxNodeSize, referencedValue);
	}

	public BTreeMap(Store store, final long pointer, Comparator<K> comparator, Serializer<K> keySerializer, Serializer<V> valueSerializer, int maxNodeSize, boolean referencedValue) throws IOException {
		this.store = store;
		this.comparator = comparator;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.referencedValue = referencedValue;
		this.maxNodeSize = maxNodeSize;

		// Stops over serialization
		this.keyCache = new LRUMap<>(maxNodeSize);

		// This can be null and can change...
		if (pointer != -1) {
			this.rootPointer = new Pointer(pointer);
		} else {
			BTreeNode root = new BTreeNode(maxNodeSize, true);
			long p = createNode(root);
			rootPointer = new Pointer(p);
		}
	}

	public V add(K key, V value) throws IOException, DuplicateKeyException {
		return add(key, value, false);
	}

	public V add(K key, V value, boolean replace) throws IOException, DuplicateKeyException {
		if (key == null && value == null) {
			throw new IllegalArgumentException("Both key and value cannot be null");
		}
		if (value == null) {
			throw new IllegalArgumentException("Value cannot be null");
		}
		// Root exists
		BTreeNode node = getNode(rootPointer.getPointer());
		// Always add root to the stack
		List<BTreeNode> nodes = new ArrayList<>(4);
		List<Long> pointers = new ArrayList<>(4);

		// Add node and pointer
		nodes.add(node);
		pointers.add(rootPointer.getPointer());
		// Find the leaf node
		while (!node.isLeaf()) {
			int pos = determinePosition(key, node.getKeys());
			if (pos < 0) {
				pos = 0;
			} else if (pos >= node.getKeys().size()) {
				pos = node.getKeys().size() - 1;
			}

			Object entry = processKey(node.getKeys().get(pos));
			if (pos == 0 && comparator.compare(key, (K) entry) < 0) {
				long pointer = ((Pointer)node.getChildren().get(0)).getPointer();
				node = getNode(pointer);
				// Push to beginning of array list
				pointers.add(0, pointer);
				nodes.add(0, node);
			} else {
				long pointer = ((Pointer)node.getChildren().get(pos + 1)).getPointer();
				node = getNode(pointer);
				// Push to beginning of array list
				pointers.add(0, pointer);
				nodes.add(0, node);
			}
		}

		// Find position to insert
		int pos = determinePosition(key, node.getKeys());

		// Add to the end
		if (pos == node.getKeys().size()) {
			node.getKeys().add(processNewKey(key));
			node.getValues().add(processValue(-1, value));
			updateNodes(nodes, key, pointers);
			return value;
		}

		// Not found, add to beginning
		if (pos < 0) {
			node.getKeys().add(0, processNewKey(key));
			node.getValues().add(0, processValue(-1, value));
			updateNodes(nodes, key, pointers);
			return value;
		}

		Object found = processKey(node.getKeys().get(pos));
		Object oldValue = node.getValues().get(pos);
		int comp = comparator.compare(key, (K)found);
		if (comp == 0) {
			if (!replace) {
				throw new DuplicateKeyException("Key: " + key + " is not unique");
			} else {
				// Reset the value
				// FIXME: This needs to handle secondary indexes properly.
				if (referencedValue) {
					Pointer p = (Pointer) oldValue;
					removeNode(p.getPointer());
				}
				node.getValues().set(pos, processValue(-1, value));
			}
		} else {
			node.getKeys().add(pos, processNewKey(key));
			node.getValues().add(pos, processValue(-1, value));
		}

		updateNodes(nodes, key, pointers);
		return value;
	}

	public V get(K key) throws IOException {
		if (key == null) {
			throw new IllegalArgumentException("Key cannot be null");
		}
		if (rootPointer == null) {
			return null;
		}

		BTreeNode node = getNode(rootPointer.getPointer());
		while (!node.isLeaf()) {
			int pos = determinePosition(key, node.getKeys());
			if (pos < 0) {
				pos = 0;
			}
			if (pos >= node.getKeys().size()) {
				// Go to the right and continue
				Pointer p = (Pointer) node.getChildren().get(node.getKeys().size());
				node = getNode(p.getPointer());
				continue;
			}

			Object entry = processKey(node.getKeys().get(pos));
			// If pos is 0 and the key is less than pos, then go right
			if (pos == 0 && comparator.compare(key, (K) entry) < 0) {
				node = getNode(((Pointer) node.getChildren().get(0)).getPointer()); // First child is left
				continue;
			} else if (pos == 0 && comparator.compare(key, (K) entry) >= 0) {
				// Second child is right, is this ever going to be run? Needs coverage check...
				node = getNode(((Pointer) node.getChildren().get(1)).getPointer());
			} else if (pos > 0 && comparator.compare(key, (K) entry) < 0) {
				node = getNode(((Pointer) node.getChildren().get(pos - 1)).getPointer()); // FIXME: Needs to be checked...
				continue;
			}
			// Go to the right of pos...
			node = getNode(((Pointer) node.getChildren().get(0)).getPointer());
		}

		int pos = determinePosition(key, node.getKeys());
		// we should always get the correct position.
		if (pos == node.getKeys().size() || pos < 0) {
			return null;
		}

		// Process the value retrieved from the node...
		return (V) getValue(node.getValues().get(pos));
	}

	public V remove(K key) {
		return null;
	}

	public V update(K key) {
		return null;
	}

	private void updateNodes(List<BTreeNode> nodes, K key, List<Long> pointers) throws IOException {
		if (nodes.size() == 0) {
			return;
		}
		// Check for split and return as there is no need to finish the update otherwise
		if (nodes.get(0).getValues().size() > maxNodeSize) {
			split(nodes, key, pointers);
			return;
		}

		long newPointer = -1, previous = -1;
		for (int i = 0; i < nodes.size(); i++) {
			BTreeNode node = nodes.get(i);
			long current = pointers.get(i);
			// If the node is a leaf, just store it.
			if (node.isLeaf()) {
				newPointer = updateNode(current, node);
				if (current == newPointer) {
					return;
				} else if (current == rootPointer.getPointer()) {
					rootPointer = new Pointer(newPointer);
					return;
				}
				previous = current;
			} else {
				// New pointer will not be -1. The fact that we are here is because the pointer of the leaf changed
				int pos = determinePosition(key, node.getKeys());
				if (pos < 0) {
					pos = 0;
				} else if (pos >= node.getKeys().size()) {
					pos = node.getKeys().size() - 1;
				}
				Object entry = processKey(node.getKeys().get(pos));
				Pointer left = (Pointer)node.getChildren().get(pos); // Zero is always left
				Pointer right = (Pointer)node.getChildren().get(pos + 1); // POS + 1 is always right

				if (pos == 0 && left.getPointer() == previous && newPointer != -1) {
					node.getChildren().set(0, new Pointer(newPointer));
				} else if (newPointer != -1) {
					node.getChildren().set(pos + 1, new Pointer(newPointer));
				}

				newPointer = updateNode(current, node);
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

	private void split(List<BTreeNode> nodes, K key, List<Long> pointers) throws IOException {
		//long oldPointer = -1;
		BTreeNode node = nodes.get(0);

		int size = node.getKeys().size();
		if (size < maxNodeSize) {
			return; // do nothing
		}
		int medianIndex = (size - 1) >>> 1;

		// Get Left entries
		List<Object> leftEntries = new ArrayList<>(size / 2);
		List<Object> leftValues = new ArrayList<>(size / 2 + 1);
		for (int i = 0; i < medianIndex; i++) {
			leftEntries.add(node.getKeys().get(i));
		}
		if (!node.isLeaf()) {
			for (int i = 0; i < (medianIndex + 1); i++) {
				leftValues.add(node.getChildren().get(i));
			}
		} else {
			for (int i = 0; i < medianIndex; i++) {
				leftValues.add(node.getValues().get(i));
			}
		}

		// Get right entries
		List<Object> rightEntries = new ArrayList<>(size / 2);
		List<Object> rightValues = new ArrayList<>(size / 2 + 1);
		for (int i = medianIndex; i < size; i++) {
			rightEntries.add(node.getKeys().get(i));
		}

		if (!node.isLeaf()) {
			for (int i = (medianIndex + 1); i < node.getChildren().size(); i++) {
				rightValues.add(node.getChildren().get(i));
			}
		} else {
			for (int i = medianIndex; i < node.getValues().size(); i++) {
				rightValues.add(node.getValues().get(i));
			}
		}

		// Get the middle value
		Object medianEntry = node.getKeys().get(medianIndex);

		// No parent... So we need to create a new root and save it's two child nodes.
		if (nodes.size() <= 1) {
			BTreeNode newRoot = new BTreeNode(maxNodeSize, false);

			// Create the left node
			BTreeNode left = new BTreeNode(maxNodeSize, node.isLeaf());
			left.getKeys().addAll(leftEntries);
			left.getValues().addAll(leftValues);

			// Create right node and add to root
			long lp = createNode(left);
			newRoot.getChildren().add(0, new Pointer(lp));

			// Create the right node
			BTreeNode right = new BTreeNode(maxNodeSize, node.isLeaf());
			right.getKeys().addAll(rightEntries);

			// Create right node and add to root
			long rp = createNode(right);
			newRoot.getChildren().add(new Pointer(rp));

			// Add the entry to the new root node
			newRoot.getKeys().add(medianEntry);

			long p = createNode(newRoot);
			rootPointer = new Pointer(p);
			// Remove the old node
			removeNode(pointers.get(0));
			return; // nothing else to do.
		}

		// There is a parent node, so we need to get that and add the relevant keys to it
		BTreeNode parent = nodes.get(1);
		int pos = determinePosition(processKey(medianEntry), parent.getKeys());

		BTreeNode rightNode = new BTreeNode(maxNodeSize, node.isLeaf());
		rightNode.setKeys(rightEntries);

		// Create a new left hand node
		BTreeNode leftNode = new BTreeNode(maxNodeSize, node.isLeaf());
		leftNode.setKeys(leftEntries);

		key = (K) processKey(medianEntry);
		BTreeEntry parentEntry = new BTreeEntry();
		parentEntry.setKey(key);

		// Store the new left hand node
		long lp = createNode(leftNode);

		if (pos <= 0) {
			BTreeEntry oldFirst = parent.getEntries().get(0);
			if (oldFirst.getLeft() != null) {
				BTreeNode oldLeft = getNode(oldFirst.getLeft().getPointer());
				rightNode.getEntries().addAll(oldLeft.getEntries());
				// Remove the old left hand node...
				removeNode(oldFirst.getLeft().getPointer());
			}
			parentEntry.setLeft(new Pointer(lp));
		} else {
			BTreeEntry leftEntry = parent.getEntries().get(pos - 1);
			leftEntry.setRight(new Pointer(lp));
		}
		// Save the right hand node
		long rp = createNode(rightNode);

		// Set the newly saved right pointer.
		parentEntry.setRight(new Pointer(rp));
		parent.getEntries().add(pos, parentEntry);

		// Remove the old node.
		removeNode(pointers.get(0));

		// Update the parent node
		long oldPointer = pointers.get(1);
		long newPointer = updateNode(pointers.get(1), parent);
		if (oldPointer != newPointer) {
			pointers.set(1, newPointer);
			removeNode(oldPointer); // remove the old parent from the cache as we don't need it.
			if (oldPointer == rootPointer.getPointer()) {
				rootPointer = new Pointer(newPointer);
			}
		}

		// Update nodes anyway
		updateNodes(nodes.subList(1, nodes.size()), key, pointers.subList(1, pointers.size()));
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
		// FIXME: Do something here
	}

	public void unlock(Long v) {
		final Thread t = locks.remove(v);
	}

	protected BTreeNode getNode(Long p) throws IOException {
		synchronized (nodeCache) {
			BTreeNode n = nodeCache.get(p);
			if (n != null) {
				return n;
			}
			n = store.get(p, nodeSerializer);
			nodeCache.put(p, n);
			return n;
		}
	}

	protected Long updateNode(Long p, BTreeNode n) throws IOException {
		long np = store.update(p, n, nodeSerializer);
		synchronized (nodeCache) {
			if (np != p) {
				//System.out.println("Node has moved");
				nodeCache.remove(p);
			}
			nodeCache.putIfAbsent(np, n);
		}
		return np;
	}

	protected Long createNode(BTreeNode n) throws IOException {
		long p = store.put(n, nodeSerializer);
		synchronized (nodeCache) {
			nodeCache.put(p, n);
		}
		return p;
	}

	protected void removeNode(long p) throws IOException {
		synchronized (nodeCache) {
			nodeCache.remove(p);
			store.remove(p);
		}
	}

	protected Object getValue(Object v) throws IOException {
		if (v instanceof Pointer && referencedValue) {
			return store.get(((Pointer) v).getPointer(), valueSerializer);
		}
		return v;
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
				long kp = store.put((K)key, keySerializer);
				return new Pointer(kp);
			}
		}
	}

	private Object processKey(Object key) throws IOException {
		if (key instanceof Pointer) {
			if (!keyCache.containsKey((Pointer)key)) {
				Object k = store.get(((Pointer) key).getPointer(), keySerializer);
				keyCache.replace((Pointer)key, key);
				return k;
			}
			return keyCache.get((Pointer)key);
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
			if (key == null) {
				return 0;
			}
			int mid = (low + high) >>> 1;
			if (low > high) {
				return high;
			}

			Object lowKey = processKey((K) entries.get(low));
			Object midKey = processKey((K) entries.get(mid));
			Object highKey = processKey((K) entries.get(high));

			if (entries.get(low) == null) {
				low++;
			}
			if (comparator.compare((K) key, (K) lowKey) < 0) {
				return low == 0 ? low : low - 1;
			}
			if (comparator.compare((K) key, (K) lowKey) == 0) {
				return low;
			}
			if (comparator.compare((K) key, (K) lowKey) > 0) {
				low++;
			}
			if (comparator.compare((K) key, (K) midKey) < 0) {
				high = mid - 1;
			}
			if (comparator.compare((K) key, (K) midKey) == 0) {
				return mid;
			}
			if (comparator.compare((K) key, (K) midKey) > 0) {
				low = mid + 1;
			}
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
		private List<Object> values; // Could be children
		private final boolean leaf;
		private final int maxNodeSize;

		public BTreeNode(int maxNodeSize, boolean leaf) {
			this.keys = new ArrayList<>(maxNodeSize);
			this.values = new ArrayList<>(maxNodeSize + 1); // First value is always left if leaf is false
			this.leaf = leaf;
			this.maxNodeSize = maxNodeSize;
		}

		public int getMaxNodeSize() {
			return maxNodeSize;
		}

		public List<Object> getKeys() {
			return keys;
		}

		public void setKeys(List<Object> keys) {
			this.keys = keys;
		}

		public List<Object> getValues() {
			if (leaf) {
				return this.values;
			}
			throw new IllegalArgumentException("Cannot set values on a branch node");
		}

		public void setValues(List<Object> values) {
			if (leaf) {
				this.values = values;
				return;
			}
			throw new IllegalArgumentException("Cannot set values on a branch node");
		}

		public List<Object> getChildren() {
			if (!leaf) {
				return this.values;
			}
			throw new IllegalArgumentException("Cannot get children on a leaf node");
		}

		public void setChildren(List<Object> children) {
			if (!leaf) {
				this.values = children;
				return;
			}
			throw new IllegalArgumentException("Cannot get children on a leaf node");
		}

		public boolean isLeaf() {
			return leaf;
		}

	}

	private static final class BTreeNodeSerializer implements Serializer<BTreeNode> {

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
