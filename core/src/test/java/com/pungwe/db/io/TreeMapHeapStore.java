package com.pungwe.db.io;

import com.pungwe.db.io.serializers.Serializer;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 15/10/2014.
 */
public class TreeMapHeapStore implements Store {

	private AtomicLong currentId = new AtomicLong();
	private Map<Long, Object> data = new TreeMap<>();

	public Map<Long, Object> getData() {
		return data;
	}

	@Override
	public <T> long put(T value, Serializer<T> serializer) throws IOException {
		long id = currentId.getAndIncrement();
		data.put(id, value);
		return id;
	}

	@Override
	public <T> T get(long position, Serializer<T> serializer) throws IOException {
		return (T) data.get(position);
	}

	@Override
	public <T> long update(long position, T value, Serializer<T> serializer) throws IOException {
		data.put(position, value);
		return position;
	}

	@Override
	public void commit() {

	}

	@Override
	public void rollback() throws UnsupportedOperationException {

	}

	@Override
	public boolean isClosed() {
		return false;
	}

	@Override
	public void lock(long position, int size) {

	}

	@Override
	public void unlock(long position, int size) {

	}

	@Override
	public void close() throws IOException {

	}

}
