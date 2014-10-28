package com.pungwe.db.types;

import com.pungwe.db.io.Store;
import com.pungwe.db.io.serializers.Serializer;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 15/10/2014.
 */
public class TreeMapHeapStore implements Store {

	private AtomicLong currentId = new AtomicLong();
	private Map<Long, Value> data = new TreeMap<>();

	private class Value {

		private Value(Object value, Serializer serializer) {
			this.value = value;
			this.serializer = serializer;
		}

		public Object getValue() {
			return value;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public Serializer getSerializer() {
			return serializer;
		}

		public void setSerializer(Serializer serializer) {
			this.serializer = serializer;
		}

		private Object value;
		private Serializer serializer;
	}

	public Map<Long, Value> getData() {
		return data;
	}

	@Override
	public <T> long put(T value, Serializer<T> serializer) throws IOException {
		long id = getNextPosition();
		data.put(id, new Value(value, serializer));
		return id;
	}

	@Override
	public <T> T get(long position, Serializer<T> serializer) throws IOException {
		if (!data.containsKey(position)) {
			return null;
		}
		return (T)data.get(position).getValue();
	}

	@Override
	public <T> long update(long position, T value, Serializer<T> serializer) throws IOException {
		if (isAppendOnly()) {
			remove(position);
			position = getNextPosition();
		}
		data.put(position, new Value(value, serializer));
		return position;
	}

	@Override
	public void remove(long position) {
		data.remove(position);
	}

	@Override
	public void commit() {
	}


	@Override
	public void rollback() throws UnsupportedOperationException {
		data.clear();
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

	private long getNextPosition() throws IOException {
		return currentId.getAndIncrement();
	}

	@Override
	public boolean isAppendOnly() {
		return false;
	}

	@Override
	public void close() throws IOException {

	}

}
