package com.pungwe.db.types.btree;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Map;

/**
 * Created by 917903 on 04/03/2015.
 */
final class EntrySet<K, V> extends AbstractSet<Map.Entry<K, V>> {

	final BTreeMap<K, V> map;
	final Object lo, hi;
	final boolean hiInclusive, loInclusive;

	public EntrySet(BTreeMap<K, V> map) {
		this.map = map;
		lo = null;
		hi = null;
		hiInclusive = false;
		loInclusive = false;
	}

	public EntrySet(BTreeMap<K, V> map, Object lo, boolean loInclusive, Object hi, boolean hiInclusive) {
		this.map = map;
		this.lo = lo;
		this.hi = hi;
		this.loInclusive = loInclusive;
		this.hiInclusive = hiInclusive;
		if (lo != null && hi != null && map.keyComparator.compare((K)lo, (K)hi) > 0) {
			throw new IllegalArgumentException();
		}
	}

	@Override
	public Iterator<Map.Entry<K, V>> iterator() {
		if (lo != null || hi != null) {
			return new BTreeNodeIterator<K, V>(map, lo, loInclusive, hi, hiInclusive);
		}
		return map.entryIterator();
	}

	@Override
	public int size() {
		return map.size();
	}
}