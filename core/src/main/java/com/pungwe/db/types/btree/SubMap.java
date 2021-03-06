package com.pungwe.db.types.btree;

import java.util.*;
import java.util.concurrent.ConcurrentNavigableMap;

/**
 * Created by 917903 on 04/03/2015.
 */
final class SubMap<K, V> extends AbstractMap<K, V> implements ConcurrentNavigableMap<K, V> {
	protected final BTreeMap<K, V> map;
	protected final Object lo, hi;
	protected final boolean loInclusive, hiInclusive;

	public SubMap(BTreeMap<K, V> m, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
		this.map = m;
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
			int c = map.keyComparator.compare(key, (K) lo);
			if (c < 0 || (c == 0 && !loInclusive))
				return true;
		}
		return false;
	}

	private boolean tooHigh(K key) {
		if (hi != null) {
			int c = map.keyComparator.compare(key, (K) hi);
			if (c > 0 || (c == 0 && !hiInclusive))
				return true;
		}
		return false;
	}


	private boolean inBounds(K key) {
		return !tooLow(key) && !tooHigh(key);
	}

	@Override
	public V get(Object key) {
		if (!inBounds((K)key)) {
			throw new IllegalArgumentException("Key not within bounds");
		}
		return map.get(key);
	}

	@Override
	public V put(K key, V value) {
		if (!inBounds((K)key)) {
			throw new IllegalArgumentException("Key not within bounds");
		}
		return map.put(key, value);
	}

	@Override
	public boolean containsKey(Object key) {
		if (!inBounds((K)key)) {
			throw new IllegalArgumentException("Key not within bounds");
		}
		return map.containsKey(key);
	}

	@Override
	public Set<Entry<K, V>> entrySet() {
		return new EntrySet<K,V>(map, lo, loInclusive, hi, hiInclusive);
	}

	@Override
	public NavigableSet<K> keySet() {
		return new KeySet<K>((BTreeMap<K, Object>)map, lo, loInclusive, hi, hiInclusive);
	}

	@Override
	public ConcurrentNavigableMap<K, V> subMap(K fromKey, boolean fromInclusive, K toKey, boolean toInclusive) {
		if (!inBounds(fromKey) || !inBounds(toKey)) {
			throw new IllegalArgumentException("Key not within bounds");
		}
		return map.subMap(fromKey, fromInclusive, toKey, toInclusive);
	}

	@Override
	public ConcurrentNavigableMap<K, V> headMap(K toKey, boolean inclusive) {
		if (!inBounds(toKey)) {
			throw new IllegalArgumentException("Key not within bounds");
		}
		return map.subMap((K) lo, true, toKey, inclusive);
	}

	@Override
	public ConcurrentNavigableMap<K, V> tailMap(K fromKey, boolean inclusive) {
		if (!inBounds(fromKey)) {
			throw new IllegalArgumentException("Key not within bounds");
		}
		return map.subMap(fromKey, inclusive, (K) hi, false);
	}

	@Override
	public ConcurrentNavigableMap<K, V> subMap(K fromKey, K toKey) {
		if (!inBounds(fromKey) || !inBounds(toKey)) {
			throw new IllegalArgumentException("Key not within bounds");
		}
		return map.subMap(fromKey, true, toKey, false);
	}

	@Override
	public ConcurrentNavigableMap<K, V> headMap(K toKey) {
		if (!inBounds(toKey)) {
			throw new IllegalArgumentException("Key not within bounds");
		}
		return map.subMap((K) lo, loInclusive, toKey, false);
	}

	@Override
	public ConcurrentNavigableMap<K, V> tailMap(K fromKey) {
		if (!inBounds(fromKey)) {
			throw new IllegalArgumentException("Key not within bounds");
		}
		return map.subMap(fromKey, true, (K) hi, hiInclusive);
	}

	@Override
	public ConcurrentNavigableMap<K, V> descendingMap() {
		return new DescendingMap<K, V>(map, (K)hi, hiInclusive, (K)lo, loInclusive);
	}

	@Override
	public NavigableSet<K> navigableKeySet() {
		return new KeySet<K>((BTreeMap<K, Object>)map, (K)lo, loInclusive, (K)hi, hiInclusive);
	}

	@Override
	public NavigableSet<K> descendingKeySet() {
		return new DescendingKeySet<K>((BTreeMap<K, Object>)map, (K)hi, hiInclusive, (K)lo, loInclusive);
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
		Entry<K,V> e = map.lowerEntry(key);
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
		Iterator<Entry<K, V>> it = map.descendingIterator(key, true, null, false);
		Entry<K, V> same = it.next();
		Entry<K, V> next = it.next();

		if (next != null && !inBounds(next.getKey())) {
			next = null;
		}
		if (next == null && same != null && comparator().compare(key, same.getKey()) >= 0) {
			return same;
		} else if (next != null && comparator().compare(key, next.getKey()) > 0) {
			return next;
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
		Iterator<Entry<K, V>> it = map.entryIterator(key, true, hi, true);
		Entry<K, V> same = it.next();
		Entry<K, V> next = it.next();
		if (next == null && same != null && comparator().compare(key, same.getKey()) <= 0) {
			return same;
		} else if (next != null && comparator().compare(key, next.getKey()) < 0) {
			return next;
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
		Entry<K,V> e = map.higherEntry(key);
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
		return map.entryIterator(lo, loInclusive, hi, hiInclusive).next();
	}

	@Override
	public Entry<K, V> lastEntry() {
		return map.descendingIterator(hi, hiInclusive, lo, loInclusive).next();
	}

	@Override
	public Entry<K, V> pollFirstEntry() {
		map.lock.writeLock().lock();
		try {
			Entry<K, V> entry = firstEntry();
			if (entry != null) {
				remove(entry.getKey());
			}
			return entry;
		} finally {
			if (map.lock.writeLock().isHeldByCurrentThread()) {
				map.lock.writeLock().unlock();
			}
		}
	}

	@Override
	public Entry<K, V> pollLastEntry() {
		map.lock.writeLock().lock();
		try {
			Entry<K, V> entry = lastEntry();
			if (entry != null) {
				remove(entry.getKey());
			}
			return entry;
		} finally {
			if (map.lock.writeLock().isHeldByCurrentThread()) {
				map.lock.writeLock().unlock();
			}
		}
	}

	@Override
	public Comparator<? super K> comparator() {
		return map.comparator();
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
}
