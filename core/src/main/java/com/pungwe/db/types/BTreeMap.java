package com.pungwe.db.types;

import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.exception.DuplicateKeyException;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.store.Store;

import java.io.ByteArrayOutputStream;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 917903 on 27/02/2015.
 */
public class BTreeMap<K,V> implements ConcurrentNavigableMap<K,V> {


	protected volatile long headerPosition;

	// Used as a locking mechanism
	protected final AtomicLong nodeId = new AtomicLong();

	protected final Store store;
	protected final Comparator<K> keyComparator;
	protected final Serializer<K> keySerializer;
	protected final Serializer<V> valueSerializer;

	public BTreeMap(Store store, Comparator<K> keyComparator, Serializer<K> keySerializer, Serializer<V> valueSerializer, boolean referenced) throws IOException {
		this.store = store;
		this.keyComparator = keyComparator;
		this.keySerializer = keySerializer;
		this.valueSerializer = valueSerializer;

		long meta = store.getHeader().getMetaData();
		if (meta < 0) {
			LeafNode<K, Long> rootNode = new LeafNode<K, Long>(this.keyComparator);
			long rootOffset = store.put(rootNode, new BTreeNodeSerializer());
			BTreeMeta bm = new BTreeMeta();
			bm.rootOffset = rootOffset;
			bm.referenced = referenced;
			bm.keySerializer = keySerializer.getClass().getName();
			bm.valueSerializer = valueSerializer.getClass().getName();
			bm.keyComparator = keyComparator.getClass().getName();
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
		return null;
	}

	@Override
	public K lastKey() {
		return null;
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
		return 0;
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
	public V get(Object key) {
		return null;
	}

	@Override
	public synchronized V put(K key, V value) {
		return null;
	}

	@Override
	public V remove(Object key) {
		return null;
	}

	@Override
	public void putAll(Map<? extends K, ? extends V> m) {

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
		return null;
	}

	@Override
	public NavigableSet<K> descendingKeySet() {
		return null;
	}

	@Override
	public V putIfAbsent(K key, V value) {
		return null;
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

	private Iterator<Entry<K,V>> entryIterator() {
		throw new UnsupportedOperationException();
	}

	static abstract class BTreeNode<K1, K2> {
		final Comparator<K1> comparator;

		public BTreeNode(Comparator<K1> comparator) {
			this.comparator = comparator;
		}

		protected volatile Object[] keys = new Object[0];

		public abstract BTreeNode<K1, K2> copyRightSplit(int mid);

		public abstract BTreeNode<K1, K2> copyLeftSplit(int mid);

		protected void addKey(int pos, K1 key) {
			Object[] newKeys = Arrays.copyOf(keys, keys.length + 1);
			if (pos < keys.length) {
				System.arraycopy(keys, pos, newKeys, pos + 1, keys.length);
			} else if (pos == keys.length) {
				System.arraycopy(keys, 0, newKeys, 0, keys.length);
			}
			newKeys[pos] = key;
			keys = newKeys;
		}

		protected void setKey(int pos, K1 key) {
			assert pos < keys.length : "Cannot overwrite a key that doesn't exist";
			keys[pos] = key;
		}

		protected K1 getKey(int pos) {
			return (K1)keys[pos];
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
			int high = keys.length;

			return findPosition(key, low, high);
		}

		protected int findPosition(K1 key, int low, int high) {

			K1 lowKey = (K1)keys[low];
			K1 highKey = (K1)keys[high];

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
			K1 midKey = (K1)keys[mid];
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

		protected volatile Object[] values;

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
				System.arraycopy(values, pos, newValues, pos + 1, values.length);
			} else if (pos == values.length) {
				System.arraycopy(values, 0, newValues, 0, values.length);
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

			return (K2)values[pos];
		}

		protected K1 getKey(int pos) {
			return (K1)keys[pos];
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
			return comp == 0 ? (K2)values[pos] : null;
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
	}

	static final class BranchNode<K1> extends BTreeNode<K1, Long> {

		// FIXME: Make this a counting btree..
		//protected final AtomicLong size = new AtomicLong();

		protected volatile long[] children;

		public BranchNode(Comparator<K1> comparator) {
			this(comparator, new long[0]);
		}
		public BranchNode(Comparator<K1> comparator, long[] children) {
			super(comparator);
			this.children = children;
		}

		// Accepts left and right children for a key
		public void putChild(K1 key, long[] child, boolean replace) throws DuplicateKeyException {
			int pos = findPosition(key);

			long left = -1;
			long right = -1;
			// We only have the right hand node
			if (child.length == 1) {
				right = child[0];
			} else if (child.length == 2) {
				left = child[0];
				right = child[1];
			} else {
				throw new IllegalArgumentException("Cannot add zero children");
			}

			// Add something to the end
			if (pos == keys.length) {
				addKey(pos, key);
				if (left >= 0) {
					addChild(pos, left);
				}
				addChild(pos + 1, right);
			}

			// Check the keys
			K1 existing = getKey(pos);
			int comp = comparator.compare(key, existing);

			if (comp == 0 && replace) {
				setKey(pos, key);
				if (left >= 0) {
					setChild(pos, left);
				}
				setChild(pos + 1, right);

			} else if (comp == 0 && !replace) {
				throw new DuplicateKeyException("Key already exists");
			} else if (comp < 0) {
				addKey(pos, key);
				if (left >= 0) {
					addChild(pos, left);
				}
				addChild(pos + 1, right);
			// FIXME: We shouldn't get the below. Let's see how code coverage comes out...
			} else if (comp > 0) {
				addKey(pos + 1, key);
				if (left >= 0) {
					addChild(pos + 1, left);
				}
				addChild(pos + 2, right);
			}
		}

		private void addChild(int pos, long child) {
			long[] newChildren = Arrays.copyOf(children, children.length + 1);
			if (pos < children.length) {
				System.arraycopy(children, pos, newChildren, pos + 1, children.length);
			} else if (pos == children.length) {
				System.arraycopy(children, 0, newChildren, 0, children.length);
			}
			newChildren[pos] = child;
			children = newChildren;
		}

		private void setChild(int pos, long child) {
			assert pos < children.length;
			children[pos] = child;
		}

		@Override
		public BTreeNode<K1, Long> copyRightSplit(int mid) {
			// Create a right hand node
			BranchNode<K1> right = new BranchNode<>(comparator);
			right.keys = Arrays.copyOfRange(keys, mid, keys.length);
			right.children = Arrays.copyOfRange(children, mid + 1, children.length);
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
	}

	static final class EntrySet<K1, V1> extends AbstractSet<Map.Entry<K1,V1>> {

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

	// We will add more as it goes along
	static final class BTreeMeta {
		protected volatile long rootOffset;
		protected volatile boolean referenced;
		protected volatile String keySerializer;
		protected volatile String valueSerializer;
		protected volatile String keyComparator;
	}

	static final class BTreeNodeSerializer implements Serializer<BTreeNode> {
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

	static final class MetaSerializer implements Serializer<BTreeMeta> {

		@Override
		public void serialize(DataOutput out, BTreeMeta value) throws IOException {
			out.writeByte(TypeReference.META.getType());
			out.writeLong(value.rootOffset);
			out.writeBoolean(value.referenced);
			out.writeUTF(value.keySerializer);
			out.writeUTF(value.valueSerializer);
			out.writeUTF(value.keyComparator);
		}

		@Override
		public BTreeMeta deserialize(DataInput in) throws IOException {
			BTreeMeta meta = new BTreeMeta();
			meta.rootOffset = in.readLong();
			meta.referenced = in.readBoolean();
			meta.keySerializer = in.readUTF();
			meta.valueSerializer = in.readUTF();
			meta.keyComparator = in.readUTF();
			return meta;
		}

		@Override
		public TypeReference getTypeReference() {
			return TypeReference.OBJECT;
		}
	}
}
