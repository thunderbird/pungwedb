/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.pungwe.db.io;

import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.types.BTree;
import com.pungwe.db.types.Header;
import org.apache.commons.collections4.map.LRUMap;

import java.io.*;

/**
 *
 * Created by ian on 02/11/2014.
 */
public class AppendOnlyFileStore implements Store {

	private final LRUMap<Long, CachedEntry> indexCache = new LRUMap<>(1000);

	private static final int PAGE_SIZE = 4096;
	private RandomAccessFile file;
	private long length;
	private final AppendOnlyHeader header;
	private volatile boolean closed = false;

	public AppendOnlyFileStore(File file) throws IOException {
		this.file = new RandomAccessFile(file, "rw");
		long length = this.file.length();
		if (length > 0) {
			this.header = findHeader();
		} else {
			this.header = new AppendOnlyHeader(PAGE_SIZE);
			this.writeHeader();
		}
	}

	private AppendOnlyHeader findHeader() throws IOException {
		long current = file.length() - 4096;
		while (current > 0) {
			byte[] buffer = new byte[PAGE_SIZE];
			this.file.read(buffer);
			byte firstByte = buffer[0];
			if (firstByte == TypeReference.HEADER.getType()) {
				ByteArrayInputStream bytes = new ByteArrayInputStream(buffer);
				DataInputStream in = new DataInputStream(bytes);
				in.skip(5);
				return new AppendOnlyFileSerializer().deserialize(in);
			}
			current -= PAGE_SIZE;
		}
		throw new IOException("Could not find file header. File could be wrong or corrupt");
	}

	private void writeHeader() throws IOException {
		// Get the first segment and write the header
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bytes);
		new AppendOnlyFileSerializer().serialize(out, this.header);
		// We must not exceed PAGE_SIZE
		byte[] data = bytes.toByteArray();
		assert data.length < PAGE_SIZE - 5 : "Header is larger than a block...";
		long position = header.getNextPosition(data.length);
		this.file.seek(position);
		this.file.write(TypeReference.HEADER.getType());
		this.file.writeInt(data.length);
		this.file.write(data);
		if (data.length < (PAGE_SIZE - 5)) {
			this.file.write(new byte[(PAGE_SIZE - 5) - data.length]);
		}
	}

	@Override
	public <T> long put(T value, Serializer<T> serializer) throws IOException {

		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bytes);
		serializer.serialize(out, value);
		byte[] data = bytes.toByteArray();
		long position = getHeader().getNextPosition(data.length);
		this.file.seek(position);
		this.file.write(TypeReference.OBJECT.getType());
		this.file.writeInt(data.length);
		this.file.write(data);
		// Update the header
		synchronized (header) {
			writeHeader();
		}

		if (value instanceof BTree.BTreeNode) {
			// return the position
			synchronized (indexCache) {
				CachedEntry entry = new CachedEntry();
				entry.setSerializer(serializer);
				entry.setValue(value);
				indexCache.put(position, entry);
			}
		}

		return position;
	}

	@Override
	public <T> T get(long position, Serializer<T> serializer) throws IOException {
		synchronized (indexCache) {
			if (indexCache.containsKey(position)) {
				return (T) indexCache.get(position).getValue();
			}
		}
		// We need to read the first few bytes
		//byte[] data = read(position);
		this.file.seek(position);
		byte b = (byte)this.file.read();
		assert TypeReference.fromType(b) != null : "Cannot determine type";
		int len = this.file.readInt();
		byte[] buf = new byte[len];
		this.file.read(buf);
		ByteArrayInputStream is = new ByteArrayInputStream(buf);
		DataInputStream in = new DataInputStream(is);
		T value = serializer.deserialize(in);
		if (value instanceof BTree.BTreeNode) {
			synchronized (indexCache) {
				CachedEntry entry = new CachedEntry();
				entry.setSerializer(serializer);
				entry.setValue(value);
				entry.setDirty(false);
				indexCache.putIfAbsent(position, entry);
			}
		}
		return value;
	}

	@Override
	public <T> long update(long position, T value, Serializer<T> serializer) throws IOException {
		synchronized (indexCache) {
			if (indexCache.containsKey(position) && value instanceof BTree.BTreeNode) {
				indexCache.remove(position);
			}
		}
		return put(value, serializer);
	}

	@Override
	public Header getHeader() {
		return header;
	}

	@Override
	public void remove(long position) {
		// do nothing
	}

	@Override
	public void commit() throws IOException {

	}

	@Override
	public void rollback() throws UnsupportedOperationException {

	}

	@Override
	public boolean isClosed() throws IOException {
		return closed;
	}

	@Override
	public void lock(long position, int size) {

	}

	@Override
	public void unlock(long position, int size) {

	}

	@Override
	public boolean isAppendOnly() {
		return true;
	}

	@Override
	public void close() throws IOException {
		this.file.close();
		closed = true;
	}

	private static class CachedEntry {
		private Serializer<?> serializer;
		private Object value;
		private volatile boolean dirty = false;

		public Serializer<?> getSerializer() {
			return serializer;
		}

		public void setSerializer(Serializer<?> serializer) {
			this.serializer = serializer;
		}

		public Object getValue() {
			return value;
		}

		public void setValue(Object value) {
			this.value = value;
		}

		public boolean isDirty() {
			return dirty;
		}

		public void setDirty(boolean dirty) {
			this.dirty = dirty;
		}
	}

	private static class AppendOnlyHeader extends Header {

		public AppendOnlyHeader(int blockSize) {
			super(blockSize, AppendOnlyFileStore.class.getName());
		}

		public AppendOnlyHeader(int blockSize, long currentPosition) {
			super(blockSize, currentPosition, AppendOnlyFileStore.class.getName());
		}
	}

	private class AppendOnlyFileSerializer implements Serializer<AppendOnlyHeader> {

		@Override
		public void serialize(DataOutput out, AppendOnlyHeader value) throws IOException {
			out.writeUTF(value.getStore());
			out.writeInt(value.getBlockSize());
			out.writeLong(value.getPosition());
			out.writeLong(value.getMetaData());
		}

		@Override
		public AppendOnlyHeader deserialize(DataInput in) throws IOException {
			String store = in.readUTF();
			assert store.equals(MemoryMappedFileStore.class.getName());
			int blockSize = in.readInt();
			long nextPosition = in.readLong();
			long metaData = in.readLong();
			AppendOnlyHeader header = new AppendOnlyHeader(blockSize, nextPosition);
			header.setMetaData(metaData);
			return header;
		}

		@Override
		public TypeReference getTypeReference() {
			return TypeReference.HEADER;
		}

	}
}
