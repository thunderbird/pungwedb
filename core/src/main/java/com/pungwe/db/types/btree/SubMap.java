package com.pungwe.db.types.btree;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Created by 917903 on 04/03/2015.
 */
final class SubMap<K, V> extends AbstractMap<K, V> implements ConcurrentNavigableMap<K, V> {
	protected final BTreeMap<K, V> m;
	protected final Object lo, hi;
	protected final boolean loInclusive, hiInclusive;

	public SubMap(BTreeMap<K, V> m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
		this.m = m;
		this.lo = lo;
		this.loInclusive = loInclusive;
		this.hi = hi;
		this.hiInclusive = hiInclusive;
		if (lo != null && hi != null && m.keyComparator.compare((K) lo, (K) hi) > 0) {
			throw new IllegalArgumentException();
		}
	}

	private boolean tooLow(K key) {
		if (lo != null) {
			int c = m.keyComparator.compare(key, (K) lo);
			if (c < 0 || (c == 0 && !loInclusive))
				return true;
		}
		return false;
	}

	private boolean tooHigh(K key) {
		if (hi != null) {
			int c = m.keyComparator.compare(key, (K) hi);
			if (c > 0 || (c == 0 && !hiInclusive))
				return true;
		}
		return false;
	}

	private boolean inBounds(K key) {
		return !tooLow(key) && !tooHigh(key);
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return new EntrySet<K, V>(m, lo, loInclusive, hi, hiInclusive);
	}

	@Override
	public NavigableSet<K> keySet() {
		return null;
	}

	@Override
	public ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
		return new SubMap<>(m, fromKey, fromInclusive, toKey, toInclusive);
	}

	@Override
	public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
		return new SubMap<>(m, null, false, toKey, inclusive);
	}

	@Override
	public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
		return new SubMap<>(m, fromKey, inclusive, null, false);
	}

	@Override
	public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
		return new SubMap<>(m, fromKey, true, toKey, false);
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
	public ConcurrentNavigableMap<K, V> descendingMap() {
		return new DescendingMap<>(m, lo, loInclusive, hi, hiInclusive);
	}

	@Override
	public NavigableSet<K> navigableKeySet() {
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
	public Comparator<? super K> comparator() {
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
}
