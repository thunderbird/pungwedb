package com.pungwe.db.types.btree;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by 917903 on 04/03/2015.
 */
abstract class BTreeNode<K, V> {
	final Comparator<K> comparator;

	public BTreeNode(Comparator<K> comparator) {
		this.comparator = comparator;
	}

	protected Object[] keys = new Object[0];

	public abstract BTreeNode<K, V> copyRightSplit(int mid);

	public abstract BTreeNode<K, V> copyLeftSplit(int mid);

	protected void addKey(int pos, K key) {
		Object[] newKeys = Arrays.copyOf(keys, keys.length + 1);
		if (pos < keys.length) {
			System.arraycopy(newKeys, pos, newKeys, (pos + 1), keys.length - pos);
		}
		newKeys[pos] = key;
		keys = newKeys;
	}

	protected void setKey(int pos, K key) {
		assert pos < keys.length : "Cannot overwrite a key that doesn't exist";
		keys[pos] = key;
	}

	protected K getKey(int pos) {
		return (K) keys[pos];
	}

	protected void removeKey(int pos) {
		Object[] newKeys = new Object[keys.length - 1];
		System.arraycopy(keys, 0, newKeys, 0, pos);
		if (pos < newKeys.length) {
			System.arraycopy(keys, pos + 1, newKeys, pos, (newKeys.length - pos));
		}
		keys = newKeys;
	}

	protected int findPosition(K key) {
		int low = 0;
		int high = keys.length - 1;
		return findPosition(key, low, high);
	}

	protected int findPosition(K key, int low, int high) {


		if (keys.length == 0) {
			return 0;
		}

		K lowKey = (K) keys[low];
		K highKey = (K) keys[high];

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
		K midKey = (K) keys[mid];
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
