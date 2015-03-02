package com.pungwe.db.types;

import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.exception.DuplicateKeyException;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.store.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Created by 917903 on 27/02/2015.
 */
public class BTreeMap<K, V> implements ConcurrentNavigableMap<K, V> {

	private static final Logger log = LoggerFactory.getLogger(BTreeMap.class);

	protected final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();

	protected final Store store;
	protected final Comparator<K> keyComparator;
	protected final Serializer<K> keySerializer;
	protected final Serializer<V> valueSerializer;
	protected final BTreeNodeSerializer nodeSerializer = new BTreeNodeSerializer();
	protected final int maxNodeSize;
	protected final boolean referenced;
	protected AtomicLong size;

	protected volatile long rootOffset;

	public BTreeMap(Store store, Comparator<K> keyComparator, Serializer<K> keySerializer, Serializer<V> valueSerializer, int maxNodeSize, boolean referenced) throws IOException {
		this(store, -1l, keyComparator, keySerializer, valueSerializer, maxNodeSize, referenced);
	}

	public BTreeMap(Store store, long rootOffset, Comparator<K> keyComparator, Serializer<K> keySerializer, Serializer<V> valueSerializer, int maxNodeSize, boolean referenced) throws IOException {
		this.store = store;
		this.keyComparator = keyComparator;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;
		this.maxNodeSize = maxNodeSize;
		this.referenced = referenced;

		if (rootOffset == -1) {
			LeafNode<K, V> root = new LeafNode<K, V>(keyComparator);
			this.rootOffset = store.put(root, nodeSerializer);
		} else {
			this.rootOffset = rootOffset;
		}
	}

	@Override
	public ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
		return null;
	}

	@Override
	public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
		return null;
	}

	@Override
	public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
		return null;
	}

	@Override
	public Comparator<? super K> comparator() {
		return keyComparator;
	}

	@Override
	public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
		return null;
	}

	@Override
	public ConcurrentNavigableMap<K, V> headMap(K toKey) {
		return null;
	}

	@Override
	public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
		return null;
	}

	@Override
	public K firstKey() {
		lock.readLock().lock();
		try {
			BTreeNode<K, ?> node = store.get(rootOffset, nodeSerializer);
			long current = rootOffset;
			while (!(node instanceof LeafNode)) {
				long child = ((BranchNode<K>)node).children[0]; // walk left
				node = store.get(child, nodeSerializer);
				current = child;
			}
			return (K)((LeafNode) node).values[0];
		} catch (IOException ex) {
			log.error("Could not retrieve first key", ex);
			return null;
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public K lastKey() {
		lock.readLock().lock();
		try {
			BTreeNode<K, ?> node = store.get(rootOffset, nodeSerializer);
			long current = rootOffset;
			while (!(node instanceof LeafNode)) {
				long child = ((BranchNode<K>)node).children[((BranchNode<K>)node).children.length]; // walk left
				node = store.get(child, nodeSerializer);
				current = child;
			}
			return (K)((LeafNode) node).values[((LeafNode)node).values.length];
		} catch (IOException ex) {
			log.error("Could not retrieve first key", ex);
			return null;
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public Entry<K, V> lowerEntry(K key) {
		return null;
	}

	@Override
	public K lowerKey(K key) {
		return null;
	}

	@Override
	public Entry<K, V> floorEntry(K key) {
		return null;
	}

	@Override
	public K floorKey(K key) {
		return null;
	}

	@Override
	public Entry<K, V> ceilingEntry(K key) {
		return null;
	}

	@Override
	public K ceilingKey(K key) {
		return null;
	}

	@Override
	public Entry<K, V> higherEntry(K key) {
		return null;
	}

	@Override
	public K higherKey(K key) {
		return null;
	}

	@Override
	public Entry<K, V> firstEntry() {
		return null;
	}

	@Override
	public Entry<K, V> lastEntry() {
		return null;
	}

	@Override
	public Entry<K, V> pollFirstEntry() {
		return null;
	}

	@Override
	public Entry<K, V> pollLastEntry() {
		return null;
	}

	@Override
	public ConcurrentNavigableMap<K, V> descendingMap() {
		return null;
	}

	@Override
	public NavigableSet<K> navigableKeySet() {
		return null;
	}

	@Override
	public int size() {
		return (int) Math.min(sizeLong(), Integer.MAX_VALUE);
	}

	public long sizeLong() {
		if (size != null) {
			return size.get();
		}
		return 0l;
	}

	private void incrementSize() {
		if (size == null) {
			size = new AtomicLong();
		}
		size.incrementAndGet();
	}

	@Override
	public boolean isEmpty() {
		return false;
	}

	@Override
	public boolean containsKey(Object key) {
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		return false;
	}

	@Override
	public V get(final Object key) {
		lock.readLock().lock();
		// Set current to root record
		long current = rootOffset;
		try {

			BTreeNode<K, ?> node = store.get(current, nodeSerializer);
			while (!(node instanceof LeafNode)) {
				current = ((BranchNode<K>) node).getChild((K) key);
				node = store.get(current, nodeSerializer);
			}

			LeafNode<K, Object> leaf = (LeafNode<K, Object>) node;
			if (leaf.hasKey((K) key)) {
				Object value = leaf.getValue((K) key);
				if (referenced) {
					return store.get((Long) value, valueSerializer);
				} else {
					return (V) value;
				}
			} else {
				return null;
			}
		} catch (IOException ex) {
			log.error("Could not add value for key: " + key, ex);
			return null;
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public V put(K key, V value) {
		return put2(key, value, true);
	}

	public V put2(final K key, final V value, boolean replace) {
		if (key == null || value == null) {
			throw new NullPointerException();
		}

		lock.writeLock().lock();

		K k = key;
		Object v = value;

		// If the value is referenced, then put into storage
		if (referenced) {
			try {
				v = store.put(value, valueSerializer);
			} catch (IOException ex) {
				log.error("Could not add value for key: " + key, ex);
				return null;
			}
		}

		// Set current to root record
		long current = rootOffset;
		try {

			long[] offsets = new long[1];
			BTreeNode<K, ?>[] nodes = new BTreeNode[1];
			int pos = 0;

			BTreeNode<K, ?> node = store.get(current, nodeSerializer);
			nodes[pos] = node;
			offsets[pos] = current;
			pos++;
			while (!(node instanceof LeafNode)) {
				current = ((BranchNode<K>) node).getChild(k);
				node = store.get(current, nodeSerializer);

				// Make sure we have space
				if (pos == offsets.length) {
					offsets = Arrays.copyOf(offsets, offsets.length + 1);
					nodes = Arrays.copyOf(nodes, nodes.length + 1);
				}

				// Add the node to the stack
				offsets[pos] = current;
				nodes[pos] = node;
				pos++;
			}

			// Last item is the leaf node
			LeafNode<K, Object> leaf = (LeafNode<K, Object>) nodes[pos - 1];
			leaf.putValue(key, v, replace);
			incrementSize();

			// Node is not safe and must be split
			if (((LeafNode<K, Object>) node).keys.length > maxNodeSize) {
				split(nodes, offsets);
				store.remove(current);
			} else {
				// Save...
				updateNodes(key, nodes, offsets);
			}

		} catch (IOException ex) {
			log.error("Could not add value for key: " + key, ex);
			return null;
		} finally {
			if (lock.writeLock().isHeldByCurrentThread()) {
				lock.writeLock().unlock();
			}
		}

		return value;
	}

	private void split(BTreeNode<K, ?>[] nodes, long[] offsets) throws IOException {
		BTreeNode<K, ?> node = nodes[nodes.length - 1];
		long offset = offsets[nodes.length - 1];
		int mid = (node.keys.length - 1) >>> 1;
		K key = node.getKey(mid);
		BTreeNode<K, ?> left = node.copyLeftSplit(mid);
		BTreeNode<K, ?> right = node.copyRightSplit(mid);
		long[] children = new long[2];
		children[0] = store.put(left, nodeSerializer);
		children[1] = store.put(right, nodeSerializer);

		// If we are already the root node, we create a new one...
		if (nodes.length == 1) {
			BranchNode<K> newRoot = new BranchNode<K>(keyComparator);
			newRoot.putChild(key, children);
			rootOffset = store.put(newRoot, nodeSerializer);
			return;
		}

		// Otherwise we find the parent.
		BranchNode<K> parent = (BranchNode<K>)nodes[nodes.length - 2];
		parent.putChild(key, children);

		if (parent.keys.length > maxNodeSize) {
			split(Arrays.copyOf(nodes, nodes.length - 1), Arrays.copyOf(offsets, offsets.length - 1));
			return;
		}

		updateNodes(key, Arrays.copyOf(nodes, nodes.length - 1), Arrays.copyOf(offsets, offsets.length - 1));

	}

	private void updateNodes(K key, BTreeNode<K, ?>[] nodes, long[] offsets) throws IOException {
		long newOffset = -1;
		for (int i = (nodes.length - 1); i >= 0; i--) {
			if (newOffset != -1 && (i + 1) < nodes.length && nodes[i] instanceof BranchNode) {
				int pos = ((BranchNode<K>)nodes[i]).findChildPosition(key);
				if (((BranchNode<K>)nodes[i]).children[pos] == offsets[i + 1]) {
					((BranchNode<K>)nodes[i]).children[pos] = newOffset;
				}
			}
			newOffset = store.update(offsets[i], nodes[i], nodeSerializer);
			if (offsets[i] == rootOffset) {
				rootOffset = newOffset;
				return;
			}
			if (newOffset == offsets[i]) {
				return;
			}
		}
	}

	@Override
	public V remove(Object key) {
		return null;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {
		if (m == null) {
			return;
		}
		Iterator<? extends Entry<? extends K, ? extends V>> it = m.entrySet().iterator();
		while (it.hasNext()) {
			Entry<K, V> e = (Entry<K, V>) it.next();
			put(e.getKey(), e.getValue());
		}
	}

	@Override
	public void clear() {

	}

	@Override
	public NavigableSet<K> keySet() {
		return null;
	}

	@Override
	public Collection<V> values() {
		return null;
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return new EntrySet<K,V>(this);
	}

	@Override
	public NavigableSet<K> descendingKeySet() {
		return null;
	}

	@Override
	public V putIfAbsent(K key, V value) {
		return put2(key, value, false);
	}

	@Override
	public boolean remove(Object key, Object value) {
		return false;
	}

	@Override
	public boolean replace(K key, V oldValue, V newValue) {
		return false;
	}

	@Override
	public V replace(K key, V value) {
		return null;
	}

	private Iterator<Entry<K, V>> entryIterator() {
		return new BTreeNodeIterator<K, V>(this);
	}

	static final class BTreeNodeIterator<K1, V1> implements Iterator<Entry<K1, V1>> {

		final BTreeMap<K1, V1> map;
		private Stack<BranchNode<K1>> stack = new Stack<>();
		private Stack<AtomicInteger> stackPos = new Stack<>();
		private LeafNode<K1, ?> leaf;
		private int leafPos = 0;

		public BTreeNodeIterator(BTreeMap<K1, V1> map) {
			this.map = map;
			try {
				pointToStart();
			} catch (IOException ex) {
				log.error("Could not find start of btree", ex);
			}
		}

		private void pointToStart() throws IOException {
			try {
				map.lock.readLock().lock();
				BTreeNode<K1, ?> node = map.store.get(map.rootOffset, map.nodeSerializer);
				while (!(node instanceof LeafNode)) {
					stack.push((BranchNode<K1>) node);
					stackPos.push(new AtomicInteger(1));
					long child = ((BranchNode<K1>) node).children[0];
					node = map.store.get(child, map.nodeSerializer);
				}
				leaf = (LeafNode<K1, ?>) node;
			} finally {
				map.lock.readLock().unlock();
			}
		}

		private void advance() throws IOException {
			try {
				map.lock.readLock().lock();

				if (leaf != null && leafPos < leaf.values.length) {
					return; // nothing to see here
				}

				leaf = null;
				leafPos = 0; // reset to 0

				if (stack.isEmpty()) {
					return; // nothing to see here
				}

				BranchNode<K1> parent = stack.peek(); // get the immediate parent

				int pos = stackPos.peek().getAndIncrement(); // get the immediate parent position.
				if (pos < parent.children.length) {
					long t = parent.children[pos];
					BTreeNode<K1, ?> child = map.store.get(t, map.nodeSerializer);
					if (child instanceof LeafNode) {
						leaf = (LeafNode<K1, V1>) child;
					} else {
						stack.push((BranchNode<K1>) child);
						stackPos.push(new AtomicInteger());
						advance();
					}
				} else {
					stack.pop(); // remove last node
					stackPos.pop();
					advance();
				}
			} finally {
				map.lock.readLock().unlock();
			}
		}

		@Override
		public boolean hasNext() {
			if (leaf != null && leafPos >= leaf.values.length) {
				try {
					advance();
				} catch (IOException ex) {
					throw new RuntimeException(ex); // FIXME: throw a runtime exception for now..
				}
			}
			return leaf != null;
		}

		@Override
		public Entry<K1, V1> next() {

			try {
				map.lock.readLock().lock();

				// if we don't have a next value, then return null;
				if (!hasNext()) {
					return null;
				}

				int pos = leafPos++;

				Object key = leaf.keys[pos];
				Object value = leaf.values[pos];

				return new BTreeEntry<K1, V1>(key, value, map);

			} finally {
				map.lock.readLock().unlock();
			}
		}
	}

	static final class BTreeEntry<K1, V1> implements Entry<K1, V1> {

		private final BTreeMap<K1, V1> map;
		private final Object key, value;

		public BTreeEntry(Object key, Object value, BTreeMap<K1, V1> map) {
			this.key = key;
			this.value = value;
			this.map = map;
		}

		@Override
		public K1 getKey() {
			return (K1)key;
		}

		@Override
		public V1 getValue() {
			try {
				if (map.referenced) {
					return map.store.get((Long)value, map.valueSerializer);
				} else {
					return (V1)value;
				}
			} catch (IOException ex) {
				log.error("Error reading value", ex);
				return null;
			}
		}

		@Override
		public V1 setValue(V1 value) {
			return map.put2((K1)key, (V1)value, true);
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			BTreeEntry that = (BTreeEntry) o;

			if (!key.equals(that.key)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return key.hashCode();
		}
	}

	static abstract class BTreeNode<K1, K2> {
		final Comparator<K1> comparator;

		public BTreeNode(Comparator<K1> comparator) {
			this.comparator = comparator;
		}

		protected Object[] keys = new Object[0];

		public abstract BTreeNode<K1, K2> copyRightSplit(int mid);

		public abstract BTreeNode<K1, K2> copyLeftSplit(int mid);

		protected void addKey(int pos, K1 key) {
			Object[] newKeys = Arrays.copyOf(keys, keys.length + 1);
			if (pos < keys.length) {
				System.arraycopy(newKeys, pos, newKeys, (pos + 1), keys.length - pos);
			}
			newKeys[pos] = key;
			keys = newKeys;
		}

		protected void setKey(int pos, K1 key) {
			assert pos < keys.length : "Cannot overwrite a key that doesn't exist";
			keys[pos] = key;
		}

		protected K1 getKey(int pos) {
			return (K1) keys[pos];
		}

		protected void removeKey(int pos) {
			Object[] newKeys = new Object[keys.length - 1];
			System.arraycopy(keys, 0, newKeys, 0, pos);
			if (pos < newKeys.length) {
				System.arraycopy(keys, pos + 1, newKeys, pos, (newKeys.length - pos));
			}
			keys = newKeys;
		}

		protected int findPosition(K1 key) {
			int low = 0;
			int high = keys.length - 1;
			return findPosition(key, low, high);
		}

		protected int findPosition(K1 key, int low, int high) {


			if (keys.length == 0) {
				return 0;
			}

			K1 lowKey = (K1) keys[low];
			K1 highKey = (K1) keys[high];

			int highComp = comparator.compare(key, highKey);
			int lowComp = comparator.compare(key, lowKey);

			// Check high
			if (highComp < 0) {
				high--;
			} else if (highComp == 0) {
				return high;
			} else if (highComp > 0) {
				return high + 1;
			}

			// Check low
			if (lowComp <= 0) {
				return low;
			} else if (lowComp > 0) {
				low++;
			}

			if (low > high) {
				return high;
			}

			int mid = (low + high) >>> 1;
			K1 midKey = (K1) keys[mid];
			int midComp = comparator.compare(key, midKey);

			// Check mid
			if (midComp > 0) {
				low = mid + 1;
			} else if (midComp == 0) {
				return mid;
			} else if (midComp < 0) {
				high = mid - 1;
			}

			return findPosition(key, low, high);
		}
	}

	static final class LeafNode<K1, K2> extends BTreeNode<K1, K2> {

		protected Object[] values;

		public LeafNode(Comparator<K1> comparator) {
			this(comparator, new Object[0]);
		}

		public LeafNode(Comparator<K1> comparator, Object[] values) {
			super(comparator);
			this.values = values;
		}

		private void addValue(int pos, K2 value) {
			Object[] newValues = Arrays.copyOf(values, values.length + 1);
			if (pos < values.length) {
				System.arraycopy(values, pos, newValues, pos + 1, values.length - pos);
			}
			newValues[pos] = value;
			values = newValues;
		}

		protected void setValue(int pos, K2 value) {
			assert pos < values.length : "Cannot overwrite a key that doesn't exist";
			values[pos] = value;
		}

		protected K2 removeValue(int pos) {
			Object[] newValues = new Object[values.length - 1];
			System.arraycopy(values, 0, newValues, 0, pos);
			if (pos < newValues.length) {
				System.arraycopy(values, pos + 1, newValues, pos, (newValues.length - pos));
			}
			values = newValues;

			return (K2) values[pos];
		}

		protected K1 getKey(int pos) {
			return (K1) keys[pos];
		}

		public K2 putValue(K1 key, K2 value, boolean replace) throws DuplicateKeyException {
			// Find out where the key should be
			int pos = findPosition(key);

			// New value...
			if (pos == keys.length) {
				addKey(pos, key);
				addValue(pos, value);
				return value;
			}

			// Get the key
			K1 existing = getKey(pos);

			// Compare the new key to the existing key
			int comp = comparator.compare(key, existing);

			// Compare the two keys
			if (comp == 0 && replace) {
				setKey(comp, key);
				setValue(comp, value);
			} else if (comp == 0 && !replace) {
				throw new DuplicateKeyException("Duplicate key found: " + key);
			} else if (comp > 0) {
				addKey(pos + 1, key);
				addValue(pos + 1, value);
			} else {
				addKey(pos, key);
				addValue(pos, value);
			}

			return value;
		}

		public K2 remove(K1 key) {
			int pos = findPosition(key);

			if (pos < keys.length) {
				K1 existing = getKey(pos);
				int comp = comparator.compare(key, existing);
				if (comp == 0) {
					removeKey(pos);
					return removeValue(pos);
				}
			}

			return null;
		}

		public K2 getValue(K1 key) {
			int pos = findPosition(key);
			// Key does not exist
			if (pos == keys.length) {
				return null;
			}

			// Check the key against the one found
			K1 existing = getKey(pos);

			int comp = comparator.compare(key, existing);

			// If it's the same, then return the value, if it's not, then throw an error
			return comp == 0 ? (K2) values[pos] : null;
		}

		@Override
		public BTreeNode<K1, K2> copyRightSplit(int mid) {
			// Create a right hand node
			LeafNode<K1, K2> right = new LeafNode<>(comparator);
			right.keys = Arrays.copyOfRange(keys, mid, keys.length);
			right.values = Arrays.copyOfRange(values, mid, values.length);
			return right;
		}

		@Override
		public BTreeNode<K1, K2> copyLeftSplit(int mid) {
			// Create a right hand node
			LeafNode<K1, K2> left = new LeafNode<>(comparator);
			left.keys = Arrays.copyOfRange(keys, 0, mid);
			left.values = Arrays.copyOfRange(values, 0, mid);
			return left;
		}

		public boolean hasKey(K1 key) {
			int pos = findPosition(key);
			if (pos == keys.length) {
				return false;
			}
			K1 found = (K1) keys[pos];
			return comparator.compare(key, found) == 0;
		}
	}

	static final class BranchNode<K1> extends BTreeNode<K1, Long> {

		// FIXME: Make this a counting btree..
		//protected final AtomicLong size = new AtomicLong();

		protected long[] children;

		public BranchNode(Comparator<K1> comparator) {
			this(comparator, new long[0]);
		}

		public BranchNode(Comparator<K1> comparator, long[] children) {
			super(comparator);
			this.children = children;
		}

		// Accepts left and right children for a key
		public void putChild(K1 key, long[] child) throws DuplicateKeyException {
			int pos = findPosition(key);

			assert child.length == 2;
			long left = child[0];
			long right = child[1];

			if (keys.length == 0) {
				addKey(0, key);
				addChild(0, left);
				addChild(1, right);
				return;
			}

			// Add something to the end
			if (pos == keys.length) {
				addKey(pos, key);
				setChild(pos, left);
				addChild(pos + 1, right);
				return;
			}

			// Check the keys
			K1 existing = getKey(pos);
			int comp = comparator.compare(key, existing);

			if (pos == 0) {
				if (comp == 0) {
					throw new DuplicateKeyException("Key already exists: " + key);
				} else if (comp > 0) {
					addKey(1, key);
					setChild(1, left);
					addChild(2, right);
				} else {
					addKey(0, key);
					addChild(0, left);
					addChild(1, right);
				}
				return;
			}

			if (comp == 0) {
				throw new DuplicateKeyException("Key already exists: " + key);
			} else if (comp < 0) {
				addKey(pos, key);
				setChild(pos, left);
				addChild(pos + 1, right);
				// FIXME: We shouldn't get the below. Let's see how code coverage comes out...
			} else if (comp > 0) {
				addKey(pos + 1, key);
				setChild(pos + 1, left);
				addChild(pos + 2, right);
			}
		}

		public long getChild(K1 key) {
			int pos = findPosition(key);
			if (pos == keys.length) {
				return children[children.length - 1];
			} else {
				K1 found = getKey(pos);
				int comp = comparator.compare(key, found);
				if (comp >= 0) {
					return children[pos + 1];
				} else {
					return children[pos]; // left
				}
			}
		}

		public void addChild(int pos, long child) {
			long[] newChildren = Arrays.copyOf(children, children.length + 1);
			if (pos < children.length) {
				int newPos = pos + 1;
				System.arraycopy(children, pos, newChildren, newPos, children.length - pos);
			}
			newChildren[pos] = child;
			children = newChildren;
		}

		public void setChild(int pos, long child) {
			assert pos < children.length;
			children[pos] = child;
		}

		@Override
		public BTreeNode<K1, Long> copyRightSplit(int mid) {
			// Create a right hand node
			BranchNode<K1> right = new BranchNode<>(comparator);
			right.keys = Arrays.copyOfRange(keys, mid + 1, keys.length);
			right.children = Arrays.copyOfRange(children, mid + 1, children.length);
			assert right.keys.length < right.children.length : "Keys and Children are equal";
			return right;
		}

		@Override
		public BTreeNode<K1, Long> copyLeftSplit(int mid) {
			// Create a right hand node
			BranchNode<K1> left = new BranchNode<>(comparator);
			left.keys = Arrays.copyOfRange(keys, 0, mid);
			left.children = Arrays.copyOfRange(children, 0, mid + 1);
			return left;
		}

		public int findChildPosition(K1 key) {
			int pos = findPosition(key);
			if (pos == keys.length) {
				return pos;
			} else {
				K1 found = getKey(pos);
				int comp = comparator.compare(key, found);
				if (comp >= 0) {
					return pos + 1;
				} else {
					return pos;
				}
			}
		}
	}

	static final class EntrySet<K1, V1> extends AbstractSet<Map.Entry<K1, V1>> {

		final BTreeMap<K1, V1> map;

		public EntrySet(BTreeMap<K1, V1> map) {
			this.map = map;
		}

		@Override
		public Iterator<Entry<K1, V1>> iterator() {
			return map.entryIterator();
		}

		@Override
		public int size() {
			return map.size();
		}
	}

	private final class BTreeNodeSerializer implements Serializer<BTreeNode<K, ?>> {
		@Override
		public void serialize(DataOutput out, BTreeNode value) throws IOException {
			out.writeBoolean(value instanceof LeafNode);
			out.writeInt(value.keys.length);
			for (Object k : value.keys) {
				keySerializer.serialize(out, (K) k);
			}
			if (value instanceof LeafNode) {
				for (Object o : ((LeafNode) value).values) {
					if (referenced) {
						out.writeLong((long) o);
					} else {
						valueSerializer.serialize(out, (V) o);
					}
				}
			} else {
				for (long o : ((BranchNode) value).children) {
					assert o > 0 : " pointer is 0";
					out.writeLong(o);
				}
			}
			return;
		}

		@Override
		public BTreeNode<K, ?> deserialize(DataInput in) throws IOException {
			boolean leaf = in.readBoolean();
			int keyLength = in.readInt();
			BTreeNode<K, ?> node = leaf ? new LeafNode<K, V>(keyComparator) : new BranchNode<K>(keyComparator);
			node.keys = new Object[keyLength];
			for (int i = 0; i < keyLength; i++) {
				node.keys[i] = keySerializer.deserialize(in);
			}
			if (leaf) {
				((LeafNode<K, V>) node).values = new Object[keyLength];
				for (int i = 0; i < keyLength; i++) {
					if (referenced) {
						((LeafNode<K, Long>) node).values[i] = in.readLong();
					} else {
						((LeafNode<K, V>) node).values[i] = valueSerializer.deserialize(in);
					}
				}
			} else {
				((BranchNode<K>) node).children = new long[keyLength + 1];
				for (int i = 0; i < ((BranchNode<K>) node).children.length; i++) {
					long offset = in.readLong();
					assert offset > 0 : "Offset is 0...";
					((BranchNode<K>) node).children[i] = offset;
				}
			}
			return node;
		}

		@Override
		public TypeReference getTypeReference() {
			return null;
		}
	}

}
