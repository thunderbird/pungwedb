package com.pungwe.db.types.btree;

import java.util.AbstractCollection;
import java.util.Iterator;

/**
 * Created by 917903 on 04/03/2015.
 */
final class Values<E> extends AbstractCollection<E> {

	final BTreeMap<?, E> map;

	public Values(BTreeMap<?, E> map) {
		this.map = map;
	}

	@Override
	public Iterator<E> iterator() {
		return null;
	}

	@Override
	public int size() {
		return 0;
	}
}