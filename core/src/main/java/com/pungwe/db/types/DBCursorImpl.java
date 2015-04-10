package com.pungwe.db.types;

import com.pungwe.db.util.collections.LinkedLongArray;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 917903 on 23/03/2015.
 */
public class DBCursorImpl<T> implements DBCursor<T> {

	private final String id;
	private final AtomicLong position = new AtomicLong(0);
	private final LinkedLongArray values;
	private final Iterator<Long> iterator;

	public DBCursorImpl(String id, LinkedLongArray values) {
		this.values = values;
		this.id = id;
		this.iterator = values.iterator();
	}

	@Override
	public String getCursorId() {
		return id;
	}

	@Override
	public Iterator<T> iterator() {
		return this;
	}

	@Override
	public boolean hasNext() {
		return iterator.hasNext();
	}

	@Override
	public T next() {
		long recordId = iterator.next();
		//T result = store.g
		return null;
	}
}
