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

// FIXME: Add modification listener

/**
 * Created by 917903 on 12/02/2015.
 */
public class BTree<K, V> {

	private static final Logger log = LoggerFactory.getLogger(BTree.class);

	private final ConcurrentMap<Long, Thread> locks = new ConcurrentHashMap<>();

	protected final Store store;
	protected final Serializer<K> keySerializer;
	protected final Serializer<V> valueSerializer;
	protected final Serializer<BTreeNode> nodeSerializer = new BTreeNodeSerializer();
	protected final Comparator<K> comparator;
	protected final boolean referencedValue;
	protected final LRUMap<Long, BTreeNode> nodeCache = new LRUMap<>(1000); // 1000 entry cache...
	protected final LRUMap<Pointer, Object> keyCache;
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
				long pointer = ((Pointer) node.getChildren().get(0)).getPointer();
				node = getNode(pointer);
				// Push to beginning of array list
				pointers.add(0, pointer);
				nodes.add(0, node);
			} else if (pos == 0 && comparator.compare(key, (K) entry) >= 0) {
				long pointer = ((Pointer) node.getChildren().get(1)).getPointer();
				node = getNode(pointer);
				// Push to beginning of array list
				pointers.add(0, pointer);
				nodes.add(0, node);
			} else {
				long pointer = ((Pointer) node.getChildren().get(pos + 1)).getPointer();
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
		int comp = comparator.compare(key, (K) found);
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
				continue;
			} else if (pos > 0 && comparator.compare(key, (K) entry) < 0) {
				node = getNode(((Pointer) node.getChildren().get(pos)).getPointer()); // FIXME: Needs to be checked...
				continue;
			}

			// Go to the right of pos...
			node = getNode(((Pointer) node.getChildren().get(pos + 1)).getPointer());
		}

		int pos = determinePosition(key, node.getKeys());
		// we should always get the correct position.
		if (pos == node.getKeys().size() || pos < 0) {
			return null;
		}

		// Process the value retrieved from the node...
		return (V) getValue(node.getValues().get(pos));
	}

	// FIXME: Add this
	public V remove(K key) throws IOException {
		return null;
	}

	public V update(K key, V value) throws IOException, DuplicateKeyException {
		return add(key, value, true);
	}

	private void updateNodes(List<BTreeNode> nodes, K key, List<Long> pointers) throws IOException {
		if (nodes.size() == 0) {
			System.out.println("Node size is 0: " + key);
			return;
		}
		// Check for split and return as there is no need to finish the update otherwise
		if (nodes.get(0).getKeys().size() > maxNodeSize) {
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
				int pos = determinePosition(key, node.getKeys());
				if (pos < 0) {
					pos = 0;
				} else if (pos >= node.getKeys().size()) {
					pos = node.getKeys().size() - 1;
				}
				Object entry = node.getKeys().get(pos);
				if (pos == 0 && ((Pointer) node.getChildren().get(0)).getPointer() == previous && newPointer >= 0) {
					node.getChildren().set(0, new Pointer(newPointer));
				} else if (pos == 0 && ((Pointer) node.getChildren().get(0)).getPointer() == previous && newPointer >= 0) {
					node.getChildren().set(1, new Pointer(newPointer));
				} else if (newPointer >= 0) {
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
			for (int i = 0; i < medianIndex + 1; i++) {
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
		for (int i = (node.isLeaf() ? medianIndex : medianIndex + 1); i < size; i++) {
			rightEntries.add(node.getKeys().get(i));
		}

		if (!node.isLeaf()) {
			for (int i = medianIndex + 1; i < node.getChildren().size(); i++) {
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
		if (nodes.size() == 1) {
			BTreeNode newRoot = new BTreeNode(maxNodeSize, false);

			// Create the left node
			BTreeNode left = new BTreeNode(maxNodeSize, node.isLeaf());
			left.getKeys().addAll(leftEntries);
			if (left.isLeaf()) {
				left.getValues().addAll(leftValues);
			} else {
				left.getChildren().addAll(leftValues);
			}

			// Create right node and add to root
			long lp = createNode(left);
			newRoot.getChildren().add(new Pointer(lp));

			// Create the right node
			BTreeNode right = new BTreeNode(maxNodeSize, node.isLeaf());
			right.getKeys().addAll(rightEntries);
			if (right.isLeaf()) {
				right.getValues().addAll(rightValues);
			} else {
				right.getChildren().addAll(rightValues);
			}

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
		if (rightNode.isLeaf()) {
			rightNode.setValues(rightValues);
		} else {
			rightNode.setChildren(rightValues);
		}

		// Create a new left hand node
		BTreeNode leftNode = new BTreeNode(maxNodeSize, node.isLeaf());
		leftNode.setKeys(leftEntries);
		if (leftNode.isLeaf()) {
			leftNode.setValues(leftValues);
		} else {
			leftNode.setChildren(leftValues);
		}

		key = (K) processKey(medianEntry);

		// Store the new left hand node
		long lp = createNode(leftNode);

		if (pos < 0) {
			Pointer p = (Pointer) parent.getChildren().get(0);
			BTreeNode oldLeft = getNode(p.getPointer());
			rightNode.getKeys().addAll(oldLeft.getKeys());
			if (!rightNode.isLeaf()) {
				rightNode.getChildren().addAll(oldLeft.getChildren());
			} else {
				rightNode.getValues().addAll(oldLeft.getValues());
			}
			removeNode(p.getPointer());
			parent.getChildren().add(0, new Pointer(lp));
		} else {
			Pointer p = (Pointer) parent.getChildren().get(pos);
			removeNode(p.getPointer());
			parent.getChildren().set(pos, new Pointer(lp));
		}
		// Save the right hand node
		long rp = createNode(rightNode);

		// Set the newly saved right pointer.
		parent.getKeys().add(pos, key);
		parent.getChildren().add(pos + 1, new Pointer(rp));

		// Update the parent node
		long oldPointer = pointers.get(1);
		// Remove the old node.
		removeNode(pointers.get(0));

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
				long kp = store.put((K) key, keySerializer);
				return new Pointer(kp);
			}
		}
	}

	private Object processKey(Object key) throws IOException {
		if (key instanceof Pointer) {
			if (!keyCache.containsKey((Pointer) key)) {
				Object k = store.get(((Pointer) key).getPointer(), keySerializer);
				keyCache.putIfAbsent((Pointer) key, key);
				return k;
			}
			return keyCache.get((Pointer) key);
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
				//high = mid - 1;
				high = mid;
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
				return this.children;
			}
			throw new IllegalArgumentException("Cannot get children on a leaf node");
		}

		public void setChildren(List<Object> children) {
			if (!leaf) {
				this.children = children;
				return;
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
			int bytesWritten = 9; // Always an initial 9 bytes

			// This should be a pretty straight forward task
			out.writeInt(value.getMaxNodeSize());
			out.writeBoolean(value.isLeaf()); // leaf node or not?
			out.writeInt(value.getKeys().size()); // write the number of entries

			// Write Keys
			Iterator<Object> it = value.getKeys().iterator();
			while (it.hasNext()) {
				Object key = it.next();
				TypeReference kt = TypeReference.forClass(key == null ? null : key.getClass());
				// Write the key
				switch (kt) {
					case BOOLEAN: {
						out.writeByte(kt.getType());
						keySerializer.serialize(out, (K) key);
						bytesWritten += 2;
						break;
					}
					case NUMBER:
					case DECIMAL: {
						out.writeByte(kt.getType());
						keySerializer.serialize(out, (K) key);
						bytesWritten += 9;
						break;
					}
					case NULL: {
						out.writeByte(TypeReference.NULL.getType());
						bytesWritten += 1;
						break;
					}
					default: {
						out.writeByte(TypeReference.POINTER.getType());
						if (key instanceof Pointer) {
							out.writeLong(((Pointer) key).getPointer());
						}
						bytesWritten += 9;
					}
				}
			}

			// Write child / value length
			if (value.isLeaf()) {
				out.writeInt(value.getValues().size());
			} else {
				out.writeInt(value.getChildren().size());
			}
			bytesWritten += 4;

			// Value iterator
			it = value.isLeaf() ? value.getValues().iterator() : value.getChildren().iterator();
			while (it.hasNext()) {
				Object key = it.next();
				TypeReference kt = TypeReference.POINTER;
				if (!(key instanceof Pointer)) {
					kt = TypeReference.forClass(value == null ? null : value.getClass());
				}
				// Write the key
				switch (kt) {
					case BOOLEAN: {
						out.writeByte(kt.getType());
						valueSerializer.serialize(out, (V) value);
						bytesWritten += 2;
						break;
					}
					case NUMBER:
					case DECIMAL: {
						out.writeByte(kt.getType());
						valueSerializer.serialize(out, (V) value);
						bytesWritten += 9;
						break;
					}
					case NULL: {
						out.writeByte(TypeReference.NULL.getType());
						bytesWritten += 1;
						break;
					}
					default: {
						out.writeByte(TypeReference.POINTER.getType());
						if (key instanceof Pointer) {
							out.writeLong(((Pointer) key).getPointer());
						}
						bytesWritten += 9;
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
