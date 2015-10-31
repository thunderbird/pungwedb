package com.pungwe.db.types;

import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.store.Store;
import com.pungwe.db.util.collections.LinkedLongArray;

import java.io.IOException;
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
	private final Serializer<T> serializer;
	private final Store store;

	public DBCursorImpl(String id, LinkedLongArray values, Store store, Serializer<T> serializer) {
		this.values = values;
		this.id = id;
		this.iterator = values.iterator();
		this.store = store;
		this.serializer = serializer;
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
		try {
			return store.get(recordId, serializer);
		} catch (IOException ex) {
			throw new IllegalArgumentException("Could not retrieve object");
		}
	}
}
