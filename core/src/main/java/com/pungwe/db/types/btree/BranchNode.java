package com.pungwe.db.types.btree;

import com.pungwe.db.exception.DuplicateKeyException;

import java.util.Arrays;
import java.util.Comparator;

/**
 * Created by 917903 on 04/03/2015.
 */
final class BranchNode<K> extends BTreeNode<K, Long> {
	// FIXME: Make this a counting btree..
	//protected final AtomicLong size = new AtomicLong();

	protected long[] children;

	public BranchNode(Comparator<K> comparator) {
		this(comparator, new long[0]);
	}

	public BranchNode(Comparator<K> comparator, long[] children) {
		super(comparator);
		this.children = children;
	}

	// Accepts left and right children for a key
	public void putChild(K key, long[] child) throws DuplicateKeyException {
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
		K existing = getKey(pos);
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

	public long getChild(K key) {
		int pos = findPosition(key);
		if (pos == keys.length) {
			return children[children.length - 1];
		} else {
			K found = getKey(pos);
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
	public BTreeNode<K, Long> copyRightSplit(int mid) {
		// Create a right hand node
		BranchNode<K> right = new BranchNode<>(comparator);
		right.keys = Arrays.copyOfRange(keys, mid + 1, keys.length);
		right.children = Arrays.copyOfRange(children, mid + 1, children.length);
		assert right.keys.length < right.children.length : "Keys and Children are equal";
		return right;
	}

	@Override
	public BTreeNode<K, Long> copyLeftSplit(int mid) {
		// Create a right hand node
		BranchNode<K> left = new BranchNode<>(comparator);
		left.keys = Arrays.copyOfRange(keys, 0, mid);
		left.children = Arrays.copyOfRange(children, 0, mid + 1);
		return left;
	}

	public int findChildPosition(K key) {
		int pos = findPosition(key);
		if (pos == keys.length) {
			return pos;
		} else {
			K found = getKey(pos);
			int comp = comparator.compare(key, found);
			if (comp >= 0) {
				return pos + 1;
			} else {
				return pos;
			}
		}
	}
}
