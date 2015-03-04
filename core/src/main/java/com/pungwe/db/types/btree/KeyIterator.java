package com.pungwe.db.types.btree;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by 917903 on 04/03/2015.
 */
final class KeyIterator<K> implements Iterator<K> {
	final Iterator<Map.Entry<K, ?>> iterator;

	public KeyIterator(Iterator<Map.Entry<K, ?>> iterator) {
		this.iterator = iterator;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public K next() {
		Map.Entry<K, ?> v = iterator.next();
		return v == null ? null : v.getKey();
	}
}