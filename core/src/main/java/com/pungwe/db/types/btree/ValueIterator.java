package com.pungwe.db.types.btree;

import java.util.Iterator;
import java.util.Map;

/**
 * Created by 917903 on 04/03/2015.
 */
final class ValueIterator<E> implements Iterator<E> {

	final Iterator<Map.Entry<?, E>> iterator;

	public ValueIterator(Iterator<Map.Entry<?, E>> iterator) {
		this.iterator = iterator;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public E next() {
		Map.Entry<?, E> v = iterator.next();
		return v == null ? null : v.getValue();
	}

}
