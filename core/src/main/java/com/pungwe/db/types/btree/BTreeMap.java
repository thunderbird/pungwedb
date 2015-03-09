package com.pungwe.db.types.btree;

import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.exception.DuplicateKeyException;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.store.Store;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
public final class BTreeMap<K, V> implements ConcurrentNavigableMap<K, V> {

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
		return new SubMap<>(this, fromKey, fromInclusive, toKey, toInclusive);
	}

	@Override
	public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
		return new SubMap<>(this, null, false, toKey, inclusive);
	}

	@Override
	public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
		return new SubMap<>(this, fromKey, inclusive, null, false);
	}

	@Override
	public Comparator<? super K> comparator() {
		return keyComparator;
	}

	@Override
	public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
		return subMap(fromKey, true, toKey, false);
	}

	@Override
	public ConcurrentNavigableMap<K, V> headMap(K toKey) {
		return headMap(toKey, false);
	}

	@Override
	public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
		return tailMap(fromKey, false);
	}

	@Override
	public K firstKey() {
		BTreeEntry<K, V> e = (BTreeEntry<K, V>) firstEntry();
		return e == null ? null : e.getKey();
	}

	@Override
	public K lastKey() {
		BTreeEntry<K, V> e = (BTreeEntry<K, V>) lastEntry();
		return e == null ? null : e.getKey();
	}

	@Override
	public Entry<K, V> lowerEntry(K key) {
		Iterator<Entry<K,V>> it = descendingIterator(null, false, key, false);
		if (it.hasNext()) {
			return it.next();
		}
		return null;
	}

	@Override
	public K lowerKey(K key) {
		Entry<K, V> entry = lowerEntry(key);
		if (entry == null) {
			return null;
		}
		return entry.getKey();
	}

	@Override
	public Entry<K, V> floorEntry(K key) {
		Iterator<Entry<K,V>> it = descendingIterator(null, false, key, true);
		Entry<K,V> same = it.next();
		Entry<K,V> next = it.next();
		if (next == null && same != null && comparator().compare(same.getKey(), key) <= 0) {
			return same;
		} else if (next != null && comparator().compare(key, next.getKey()) < 0) {
			return next;
		}
		return null;
	}

	@Override
	public K floorKey(K key) {
		Entry<K,V> entry = floorEntry(key);
		return entry == null ? null : entry.getKey();
	}

	@Override
	public Entry<K, V> ceilingEntry(K key) {
		Iterator<Entry<K,V>> it = entryIterator(key, true, null, false);
		Entry<K,V> same = it.next();
		Entry<K,V> next = it.next();
		if (next == null && same != null && comparator().compare(same.getKey(), key) >= 0) {
			return same;
		} else if (next != null && comparator().compare(key, next.getKey()) > 0) {
			return next;
		}
		return null;
	}

	@Override
	public K ceilingKey(K key) {
		Entry<K,V> entry = ceilingEntry(key);
		return entry == null ? null : entry.getKey();
	}

	@Override
	public Entry<K, V> higherEntry(K key) {
		Iterator<Entry<K,V>> it = entryIterator(key, false, null, false);
		if (it.hasNext()) {
			return it.next();
		}
		return null;
	}

	@Override
	public K higherKey(K key) {
		Entry<K,V> entry = higherEntry(key);
		return entry == null ? null : entry.getKey();
	}

	@Override
	public Entry<K, V> firstEntry() {
		lock.readLock().lock();
		try {
			BTreeNode<K, ?> node = store.get(rootOffset, nodeSerializer);
			long current = rootOffset;
			while (!(node instanceof LeafNode)) {
				long child = ((BranchNode<K>) node).children[0]; // walk left
				node = store.get(child, nodeSerializer);
				current = child;
			}
			return new BTreeEntry<>(node.keys[0], ((LeafNode) node).values[0], this);
		} catch (IOException ex) {
			log.error("Could not retrieve first key", ex);
			return null;
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public Entry<K, V> lastEntry() {
		lock.readLock().lock();
		try {
			BTreeNode<K, ?> node = store.get(rootOffset, nodeSerializer);
			long current = rootOffset;
			while (!(node instanceof LeafNode)) {
				long child = ((BranchNode<K>) node).children[((BranchNode<K>) node).children.length]; // walk right
				node = store.get(child, nodeSerializer);
				current = child;
			}
			return new BTreeEntry<>(node.keys[node.keys.length], ((LeafNode) node).values[((LeafNode) node).values.length - 1], this);
		} catch (IOException ex) {
			log.error("Could not retrieve first key", ex);
			return null;
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public Entry<K, V> pollFirstEntry() {
		lock.writeLock().lock();
		try {
			Entry<K, V> entry = firstEntry();
			if (entry != null) {
				remove(entry.getKey());
			}
			return entry;
		} finally {
			if (lock.writeLock().isHeldByCurrentThread()) {
				lock.writeLock().unlock();
			}
		}
	}

	@Override
	public Entry<K, V> pollLastEntry() {
		Entry<K,V> entry = lastEntry();
		if (entry != null) {
			remove(entry.getKey());
		}
		return entry;
	}

	@Override
	public ConcurrentNavigableMap<K, V> descendingMap() {
		return new DescendingMap<K, V>(this);
	}

	@Override
	public NavigableSet<K> navigableKeySet() {
		return new KeySet<K>((BTreeMap<K, Object>)this);
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
		return sizeLong() == 0;
	}

	@Override
	public boolean containsKey(Object key) {
		try {
			lock.readLock().lock();
			return findLeaf((K) key).hasKey((K) key);
		} catch (IOException ex) {
			log.error("Could not find entry");
			throw new RuntimeException(ex);
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public boolean containsValue(Object value) {
		if (value == null) {
			throw new NullPointerException();
		}
		try {
			lock.readLock().lock();
			Iterator<Map.Entry<K, V>> iterator = entryIterator();
			while (iterator.hasNext()) {
				if (iterator.next().getValue().equals(value)) {
					return true;
				}
			}
			return false;
		} finally {
			lock.readLock().unlock();
		}
	}

	@Override
	public V get(final Object key) {
		lock.readLock().lock();
		try {
			LeafNode<K, Object> leaf = (LeafNode<K, Object>) findLeaf((K) key);
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

	private LeafNode<K, ?> findLeaf(K key) throws IOException {
		long current = rootOffset;
		BTreeNode<K, ?> node = store.get(current, nodeSerializer);
		while (!(node instanceof LeafNode)) {
			current = ((BranchNode<K>) node).getChild((K) key);
			node = store.get(current, nodeSerializer);
		}
		return (LeafNode<K, ?>) node;
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
		BranchNode<K> parent = (BranchNode<K>) nodes[nodes.length - 2];
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
				int pos = ((BranchNode<K>) nodes[i]).findChildPosition(key);
				if (((BranchNode<K>) nodes[i]).children[pos] == offsets[i + 1]) {
					((BranchNode<K>) nodes[i]).children[pos] = newOffset;
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
		return new KeySet<K>((BTreeMap<K, Object>)this);
	}

	@Override
	public Collection<V> values() {
		return new Values<>(this);
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return new EntrySet<K, V>(this);
	}

	@Override
	public NavigableSet<K> descendingKeySet() {
		return new DescendingKeySet<K>((BTreeMap<K, Object>)this);
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
		return replace(key, newValue) != null;
	}

	@Override
	public V replace(K key, V value) {
		if (!containsKey(key)) {
			return null;
		}
		return put2(key, value, true);
	}

	Iterator<Map.Entry<K, V>> entryIterator() {
		return new BTreeNodeIterator<>(this);
	}

	Iterator<Map.Entry<K, V>> entryIterator(Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
		return new BTreeNodeIterator<>(this, lo, loInclusive, hi, hiInclusive);
	}

	Iterator<Map.Entry<K, V>> descendingIterator() {
		return new DescendingBTreeNodeIterator<>(this);
	}

	Iterator<Map.Entry<K, V>> descendingIterator(Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
		return new DescendingBTreeNodeIterator<>(this, lo, loInclusive, hi, hiInclusive);
	}

	public ConcurrentNavigableMap<K, V> descendingMap(K lo, boolean loInclusive, K hi, boolean hiInclusive) {
		return new DescendingMap<K, V>(this, lo, loInclusive, hi, hiInclusive);
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
