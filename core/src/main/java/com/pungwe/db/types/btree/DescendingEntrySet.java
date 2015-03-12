package com.pungwe.db.types.btree;

import java.util.*;

/**
 * Created by 917903 on 05/03/2015.
 */
final class DescendingEntrySet<K,V> extends AbstractSet<Map.Entry<K, V>> implements NavigableSet<Map.Entry<K,V>> {

	final BTreeMap<K, V> map;
	final Object lo, hi;
	final boolean hiInclusive, loInclusive;

	public DescendingEntrySet(BTreeMap<K, V> map) {
		this.map = map;
		lo = null;
		hi = null;
		hiInclusive = false;
		loInclusive = false;
	}

	public DescendingEntrySet(BTreeMap<K, V> map, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
		this.map = map;
		this.lo = lo;
		this.hi = hi;
		this.loInclusive = loInclusive;
		this.hiInclusive = hiInclusive;
		if (lo != null && hi != null && map.keyComparator.compare((K) lo, (K) hi) < 0) {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public Map.Entry<K, V> lower(Map.Entry<K, V> kvEntry) {
		return null;
	}

	@Override
	public Map.Entry<K, V> floor(Map.Entry<K, V> kvEntry) {
		return null;
	}

	@Override
	public Map.Entry<K, V> ceiling(Map.Entry<K, V> kvEntry) {
		return null;
	}

	@Override
	public Map.Entry<K, V> higher(Map.Entry<K, V> kvEntry) {
		return null;
	}

	@Override
	public Map.Entry<K, V> pollFirst() {
		return null;
	}

	@Override
	public Map.Entry<K, V> pollLast() {
		return null;
	}

	@Override
	public Iterator<Map.Entry<K, V>> iterator() {
		if (lo != null || hi != null) {
			return new DescendingBTreeNodeIterator<K, V>(map, lo, loInclusive, hi, hiInclusive);
		}
		return map.entryIterator();
	}

	@Override
	public NavigableSet<Map.Entry<K, V>> descendingSet() {
		return null;
	}

	@Override
	public Iterator<Map.Entry<K, V>> descendingIterator() {
		return null;
	}

	@Override
	public NavigableSet<Map.Entry<K, V>> subSet(Map.Entry<K, V> fromElement, boolean fromInclusive, Map.Entry<K, V> toElement, boolean toInclusive) {
		return null;
	}

	@Override
	public NavigableSet<Map.Entry<K, V>> headSet(Map.Entry<K, V> toElement, boolean inclusive) {
		return null;
	}

	@Override
	public NavigableSet<Map.Entry<K, V>> tailSet(Map.Entry<K, V> fromElement, boolean inclusive) {
		return null;
	}

	@Override
	public Comparator<? super Map.Entry<K, V>> comparator() {
		return null;
	}

	@Override
	public SortedSet<Map.Entry<K, V>> subSet(Map.Entry<K, V> fromElement, Map.Entry<K, V> toElement) {
		return null;
	}

	@Override
	public SortedSet<Map.Entry<K, V>> headSet(Map.Entry<K, V> toElement) {
		return null;
	}

	@Override
	public SortedSet<Map.Entry<K, V>> tailSet(Map.Entry<K, V> fromElement) {
		return null;
	}

	@Override
	public Map.Entry<K, V> first() {
		return null;
	}

	@Override
	public Map.Entry<K, V> last() {
		return null;
	}

	@Override
	public int size() {
		return map.size();
	}
}