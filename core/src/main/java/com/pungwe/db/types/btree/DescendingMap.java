package com.pungwe.db.types.btree;

import java.util.AbstractMap;
import java.util.Comparator;
import java.util.NavigableSet;
import java.util.Set;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Created by 917903 on 04/03/2015.
 */
final class DescendingMap<K, V> extends AbstractMap<K,V> implements ConcurrentNavigableMap<K, V> {

	final BTreeMap<K, V> map;
	final Object lo, hi;
	final boolean loInclusive, hiInclusive;

	public DescendingMap(BTreeMap<K, V> m) {
		map = m;
		lo = null;
		hi = null;
		loInclusive = false;
		hiInclusive = false;
	}

	public DescendingMap(BTreeMap<K, V> m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
		map = m;
		this.lo = lo;
		this.hi = hi;
		this.loInclusive = loInclusive;
		this.hiInclusive = hiInclusive;
		if (lo != null && hi != null && map.keyComparator.compare((K) lo, (K) hi) < 0) {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return new DescendingEntrySet<K,V>(map, lo, loInclusive, hi, hiInclusive);
	}

	@Override
	public NavigableSet<K> keySet() {
		return new DescendingKeySet<K>((BTreeMap<K, Object>)map, lo, loInclusive, hi, hiInclusive);
	}

	@Override
	public ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
		if (!inBounds(fromKey) || !inBounds(toKey)) {
			throw new IllegalArgumentException("Key not within bounds");
		}
		return new DescendingMap<K,V>(map, fromKey, fromInclusive, toKey, toInclusive);
	}

	@Override
	public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
		if (!inBounds(toKey)) {
			throw new IllegalArgumentException("Key not within bounds");
		}
		return map.descendingMap((K) lo, true, toKey, inclusive);
	}

	@Override
	public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
		if (!inBounds(fromKey)) {
			throw new IllegalArgumentException("Key not within bounds");
		}
		return map.descendingMap(fromKey, inclusive, (K) hi, false);
	}

	@Override
	public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
		if (!inBounds(fromKey) || !inBounds(toKey)) {
			throw new IllegalArgumentException("Key not within bounds");
		}
		return map.descendingMap(fromKey, true, toKey , false);
	}

	@Override
	public ConcurrentNavigableMap<K, V> headMap(K toKey) {
		if (!inBounds(toKey)) {
			throw new IllegalArgumentException("Key not within bounds");
		}
		return map.descendingMap((K)lo, loInclusive, toKey , false);
	}

	@Override
	public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
		if (!inBounds(fromKey)) {
			throw new IllegalArgumentException("Key not within bounds");
		}
		return map.descendingMap(fromKey, true, (K)hi , loInclusive);
	}

	@Override
	public ConcurrentNavigableMap<K, V> descendingMap() {
		return new SubMap<>(map, (K)hi, hiInclusive, (K)lo, loInclusive);
	}

	@Override
	public NavigableSet<K> navigableKeySet() {
		return new DescendingKeySet<K>((BTreeMap<K, Object>)map, (K)lo, loInclusive, (K)hi, hiInclusive);
	}

	@Override
	public NavigableSet<K> descendingKeySet() {
		return new KeySet<K>((BTreeMap<K, Object>)map, (K)hi, hiInclusive, (K)lo, loInclusive);
	}

	@Override
	public V putIfAbsent(K key, V value) {
		if (!inBounds(key)) {
			throw new IllegalArgumentException("Key is not within bounds");
		}
		return map.putIfAbsent(key, value);
	}

	@Override
	public boolean remove(Object key, Object value) {
		if (!inBounds((K)key)) {
			throw new IllegalArgumentException("Key is not within bounds");
		}
		return map.remove(key, value);
	}

	@Override
	public boolean replace(K key, V oldValue, V newValue) {
		if (!inBounds(key)) {
			throw new IllegalArgumentException("Key is not within bounds");
		}
		return map.replace(key, oldValue, newValue);
	}

	@Override
	public V replace(K key, V value) {
		if (!inBounds(key)) {
			throw new IllegalArgumentException("Key is not within bounds");
		}
		return map.replace(key, value);
	}

	@Override
	public Entry<K, V> lowerEntry(K key) {
		if (!inBounds(key)) {
			throw new IllegalArgumentException("Key is not within bounds");
		}
		Entry<K,V> e = map.higherEntry(key);
		if (e != null && inBounds(e.getKey())) {
			return e;
		}
		return null;
	}

	@Override
	public K lowerKey(K key) {
		Entry<K,V> e = lowerEntry(key);
		return e != null ? e.getKey() : null;
	}

	@Override
	public Entry<K, V> floorEntry(K key) {
		if (!inBounds(key)) {
			throw new IllegalArgumentException("Key is not within bounds");
		}
		Entry<K,V> e = map.ceilingEntry(key);
		if (e != null && inBounds(e.getKey())) {
			return e;
		}
		return null;
	}

	@Override
	public K floorKey(K key) {
		Entry<K,V> e = floorEntry(key);
		return e != null ? e.getKey() : null;
	}

	@Override
	public Entry<K, V> ceilingEntry(K key) {
		if (!inBounds(key)) {
			throw new IllegalArgumentException("Key is not within bounds");
		}
		Entry<K,V> e = map.floorEntry(key);
		if (e != null && inBounds(key)) {
			return e;
		}
		return null;
	}

	@Override
	public K ceilingKey(K key) {
		Entry<K,V> e = ceilingEntry(key);
		return e != null ? e.getKey() : null;
	}

	@Override
	public Entry<K, V> higherEntry(K key) {
		if (!inBounds(key)) {
			throw new IllegalArgumentException("Key is not within bounds");
		}
		Entry<K,V> e = map.lowerEntry(key);
		if (e != null && inBounds(e.getKey())) {
			return e;
		}
		return null;
	}

	@Override
	public K higherKey(K key) {
		Entry<K,V> e = higherEntry(key);
		return e != null ? e.getKey() : null;
	}

	@Override
	public Entry<K, V> firstEntry() {
		return map.descendingIterator(lo, loInclusive, hi, hiInclusive).next();
	}

	@Override
	public Entry<K, V> lastEntry() {
		return map.entryIterator(hi, hiInclusive, lo, loInclusive).next();
	}

	@Override
	public Entry<K, V> pollFirstEntry() {
//		map.lock.writeLock().lock();
		try {
			Entry<K, V> entry = firstEntry();
			if (entry != null) {
				remove(entry.getKey());
			}
			return entry;
		} finally {
//			if (map.lock.writeLock().isHeldByCurrentThread()) {
//				map.lock.writeLock().unlock();
//			}
		}
	}

	@Override
	public Entry<K, V> pollLastEntry() {
//		map.lock.writeLock().lock();
		try {
			Entry<K, V> entry = lastEntry();
			if (entry != null) {
				remove(entry.getKey());
			}
			return entry;
		} finally {
//			if (map.lock.writeLock().isHeldByCurrentThread()) {
//				map.lock.writeLock().unlock();
//			}
		}
	}

	@Override
	public Comparator<? super K> comparator() {
		return new Comparator<K>() {
			@Override
			public int compare(K o1, K o2) {
				return -map.comparator().compare(o1, o2);
			}
		};
	}

	@Override
	public K firstKey() {
		Entry<K, V> e = firstEntry();
		return e != null ? e.getKey() : null;
	}

	@Override
	public K lastKey() {
		Entry<K, V> e = lastEntry();
		return e != null ? e.getKey() : null;
	}

	private boolean tooLow(K key) {
		if (lo != null) {
			int c = map.keyComparator.compare(key, (K) lo);
			if (c > 0 || (c == 0 && !loInclusive))
				return true;
		}
		return false;
	}

	private boolean tooHigh(K key) {
		if (hi != null) {
			int c = map.keyComparator.compare(key, (K) hi);
			if (c < 0 || (c == 0 && !hiInclusive))
				return true;
		}
		return false;
	}

	private boolean inBounds(K key) {
		return !tooLow(key) && !tooHigh(key);
	}
}