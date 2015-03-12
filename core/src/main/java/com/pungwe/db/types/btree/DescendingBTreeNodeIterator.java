package com.pungwe.db.types.btree;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by 917903 on 04/03/2015.
 */
final class DescendingBTreeNodeIterator<K, V> implements Iterator<Map.Entry<K, V>> {

	final static Logger log = LoggerFactory.getLogger(DescendingBTreeNodeIterator.class);

	final BTreeMap<K, V> map;
	private Stack<BranchNode<K>> stack = new Stack<>();
	private Stack<AtomicInteger> stackPos = new Stack<>();
	private LeafNode<K, ?> leaf;
	private int leafPos = 0;
	private final Object hi;
	private final boolean hiInclusive;

	public DescendingBTreeNodeIterator(BTreeMap<K, V> map) {
		this.map = map;
		hi = null;
		hiInclusive = false;
		try {
			pointToStart();
		} catch (IOException ex) {
			log.error("Could not find start of btree", ex);
			throw new RuntimeException(ex);
		}
	}

	public DescendingBTreeNodeIterator(BTreeMap<K, V> map, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
		this.map = map;
		this.hi = hi;
		this.hiInclusive = hiInclusive;
		try {

			if (lo == null) {
				pointToStart();
			} else {
				// Find the starting point
				findLeaf((K)lo);
				int pos = leaf.findPosition((K)lo);
				K k = leaf.getKey(pos);
				int comp = map.keyComparator.compare((K) lo, k);
				if (comp < 0) {
					leafPos = pos;
				} else if (comp == 0) {
					leafPos = loInclusive ? pos : pos - 1;
				} else if (comp > 0) {
					leafPos = pos;
				}

				if (leafPos == -1) {
					advance();
				}
			}

			if (hi != null && leaf != null) {
				//check in bounds
				//int c = leaf.compare(m.keySerializer, currentPos, hi);
				int c = map.keyComparator.compare(leaf.getKey(leafPos), (K) hi);
				if (c < 0 || (c == 0 && !hiInclusive)) {
					//out of high bound
					leaf = null;
					leafPos = -1;
					//$DELAY$
				}
			}
		} catch (IOException ex) {
			log.error("Could not find start of btree");
			throw new RuntimeException(ex);
		}
	}

	private void findLeaf(K key) throws IOException {
		long current = map.rootId;
		BTreeNode<K, ?> node = map.store.get(current, map.nodeSerializer);
		while (!(node instanceof LeafNode)) {
			stack.push((BranchNode<K>) node);
			int pos = ((BranchNode<K>) node).findChildPosition((K) key);
			stackPos.push(new AtomicInteger(pos - 1));
			current = ((BranchNode<K>) node).children[pos];
			node = map.store.get(current, map.nodeSerializer);
		}
		leaf = (LeafNode<K, ?>) node;
	}

	private void pointToStart() throws IOException {
		try {
			//map.lock.readLock().lock();
			BTreeNode<K, ?> node = map.store.get(map.rootId, map.nodeSerializer);
			while (!(node instanceof LeafNode)) {
				stack.push((BranchNode<K>) node);
				stackPos.push(new AtomicInteger(((BranchNode<K>) node).children.length - 2));
				long child = ((BranchNode<K>) node).children[((BranchNode<K>) node).children.length - 1];
				node = map.store.get(child, map.nodeSerializer);
			}
			leaf = (LeafNode<K, ?>) node;
			leafPos = leaf.keys.length - 1;
		} finally {
			//map.lock.readLock().unlock();
		}
	}

	private void advance() throws IOException {
		try {
			map.lock.readLock().lock();

			if (leaf != null && leafPos > 0) {
				return; // nothing to see here
			}

			leaf = null;
			leafPos = -1; // reset to 0

			if (stack.isEmpty()) {
				return; // nothing to see here
			}

			BranchNode<K> parent = stack.peek(); // get the immediate parent

			int pos = stackPos.peek().getAndDecrement(); // get the immediate parent position.
			if (pos >= 0) {
				long t = parent.children[pos];
				BTreeNode<K, ?> child = map.store.get(t, map.nodeSerializer);
				if (child instanceof LeafNode) {
					leaf = (LeafNode<K, V>) child;
					leafPos = leaf.keys.length - 1;
				} else {
					stack.push((BranchNode<K>) child);
					stackPos.push(new AtomicInteger(((BranchNode<K>) child).children.length - 1));
					advance();
					return;
				}
			} else {
				stack.pop(); // remove last node
				stackPos.pop();
				advance();
				return;
			}

			if (hi != null && leaf != null) {
				int comp = map.keyComparator.compare(leaf.getKey(leafPos), (K) hi);
				if (comp < 0 || (comp == 0 && !hiInclusive)) {
					leaf = null;
					leafPos = -1;
				}
			}
		} finally {
			map.lock.readLock().unlock();
		}
	}

	@Override
	public boolean hasNext() {
		if (leaf != null && leafPos < 0) {
			try {
				advance();
			} catch (IOException ex) {
				throw new RuntimeException(ex); // FIXME: throw a runtime exception for now..
			}
		} else if (hi != null && leaf != null) {
			int comp = map.keyComparator.compare(leaf.getKey(leafPos), (K) hi);
			if (comp < 0 || (comp == 0 && !hiInclusive)) {
				leaf = null;
				leafPos = -1;
			}
		}
		return leaf != null;
	}

	@Override
	public Map.Entry<K, V> next() {

		try {
			//map.lock.readLock().lock();

			// if we don't have a next value, then return null;
			if (!hasNext()) {
				return null;
			}

			int pos = leafPos--;

			Object key = leaf.keys[pos];
			Object value = leaf.values[pos];

			return new BTreeEntry<K, V>(key, value, map);

		} finally {
			//map.lock.readLock().unlock();
		}
	}
}