package com.pungwe.db.types.btree;

import com.pungwe.db.exception.DuplicateKeyException;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by 917903 on 04/03/2015.
 */
final class LeafNode<K, V> extends BTreeNode<K, V> {
	protected Object[] values;

	public LeafNode(Comparator<K> comparator) {
		this(comparator, new Object[0]);
	}

	public LeafNode(Comparator<K> comparator, Object[] values) {
		super(comparator);
		this.values = values;
	}

	private void addValue(int pos, V value) {
		Object[] newValues = Arrays.copyOf(values, values.length + 1);
		if (pos < values.length) {
			System.arraycopy(values, pos, newValues, pos + 1, values.length - pos);
		}
		newValues[pos] = value;
		values = newValues;
	}

	protected void setValue(int pos, V value) {
		assert pos < values.length : "Cannot overwrite a key that doesn't exist";
		values[pos] = value;
	}

	protected V removeValue(int pos) {
		Object[] newValues = new Object[values.length - 1];
		System.arraycopy(values, 0, newValues, 0, pos);
		if (pos < newValues.length) {
			System.arraycopy(values, pos + 1, newValues, pos, (newValues.length - pos));
		}
		values = newValues;

		return (V) values[pos];
	}

	protected K getKey(int pos) {
		return (K) keys[pos];
	}

	public V putValue(K key, V value, boolean replace) throws DuplicateKeyException {
		// Find out where the key should be
		int pos = findPosition(key);

		// New value...
		if (pos == keys.length) {
			addKey(pos, key);
			addValue(pos, value);
			return value;
		}

		// Get the key
		K existing = getKey(pos);

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

	public V remove(K key) {
		int pos = findPosition(key);

		if (pos < keys.length) {
			K existing = getKey(pos);
			int comp = comparator.compare(key, existing);
			if (comp == 0) {
				removeKey(pos);
				return removeValue(pos);
			}
		}

		return null;
	}

	public V getValue(K key) {
		int pos = findPosition(key);
		// Key does not exist
		if (pos == keys.length) {
			return null;
		}

		// Check the key against the one found
		K existing = getKey(pos);

		int comp = comparator.compare(key, existing);

		// If it's the same, then return the value, if it's not, then throw an error
		return comp == 0 ? (V) values[pos] : null;
	}

	@Override
	public BTreeNode<K, V> copyRightSplit(int mid) {
		// Create a right hand node
		LeafNode<K, V> right = new LeafNode<>(comparator);
		right.keys = Arrays.copyOfRange(keys, mid, keys.length);
		right.values = Arrays.copyOfRange(values, mid, values.length);
		return right;
	}

	@Override
	public BTreeNode<K, V> copyLeftSplit(int mid) {
		// Create a right hand node
		LeafNode<K, V> left = new LeafNode<>(comparator);
		left.keys = Arrays.copyOfRange(keys, 0, mid);
		left.values = Arrays.copyOfRange(values, 0, mid);
		return left;
	}

	public boolean hasKey(K key) {
		int pos = findPosition(key);
		if (pos == keys.length) {
			return false;
		}
		K found = (K) keys[pos];
		return comparator.compare(key, found) == 0;
	}
}
