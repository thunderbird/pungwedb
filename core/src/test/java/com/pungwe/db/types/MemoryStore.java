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
package com.pungwe.db.types;

import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.io.Memory;
import com.pungwe.db.io.Store;
import com.pungwe.db.io.serializers.Serializer;
import org.apache.commons.collections4.map.LRUMap;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Created by ian on 31/10/2014.
 */
public class MemoryStore implements Store {

	private final Memory memory;
	private final MemoryHeader header;
	private final AtomicBoolean closed = new AtomicBoolean(false);
	private final double PADDING = 1.5d;

	public MemoryStore(long size) {
		memory = Memory.allocate(size);
		header = new MemoryHeader(4096, "MemoryStore");
	}

	@Override
	public <T> long put(T value, Serializer<T> serializer) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bytes);
		serializer.serialize(out, value);
		byte[] data = bytes.toByteArray();
		double length = data.length * PADDING;
		int pages = (int) Math.ceil(length / header.getBlockSize());
		long position = getHeader().getNextPosition(pages * header.getBlockSize());
		synchronized (memory) {
			memory.setByte(position, TypeReference.OBJECT.getType());
			memory.setInt(position + 1, data.length);
			memory.setBytes(position + 5, ByteBuffer.wrap(data));
		}

		return position;
	}

	@Override
	public <T> T get(long position, Serializer<T> serializer) throws IOException {

		synchronized (memory) {
			byte t = memory.getByte(position);
			assert TypeReference.fromType(t) != null : "Cannot determine type of: " + t + " at position: " + position;
			int len = memory.getInt(position + 1);
			byte[] data = new byte[len];
			memory.getBytes(position + 5, data, 0, len);
			ByteArrayInputStream is = new ByteArrayInputStream(data);
			DataInputStream in = new DataInputStream(is);
			T value = serializer.deserialize(in);

			return value;
		}
	}

	@Override
	public <T> long update(long position, T value, Serializer<T> serializer) throws IOException {
		int len = 0;
		synchronized (memory) {
			byte t = memory.getByte(position);
			assert TypeReference.fromType(t) != null : "Cannot determine type of: " + t + " at position: " + position;
			len = memory.getInt(position + 1);
		}

		int origPageSize = (int) Math.ceil(((double)len + 5) / header.getBlockSize());

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bytesOut);
		serializer.serialize(out, value);
		byte[] bytes = bytesOut.toByteArray();
		int newPageSize = (int)Math.ceil(((double)bytes.length + 5) / header.getBlockSize());

		if (newPageSize > origPageSize) {
			//System.out.println("New Page Size is bigger: " + newPageSize + " than: " + origPageSize);
			position = getHeader().getNextPosition(newPageSize * header.getBlockSize());
		}

		//write(position, bytes, TypeReference.OBJECT);

		synchronized (memory) {
			memory.setByte(position, TypeReference.OBJECT.getType());
			memory.setInt(position + 1, bytes.length);
			memory.setBytes(position + 5, ByteBuffer.wrap(bytes));
		}

		return position;
	}

	@Override
	public Header getHeader() {
		return header;
	}

	@Override
	public void remove(long position) {
		synchronized (memory) {
			byte t = memory.getByte(position);
			assert TypeReference.fromType(t) != null : "Cannot determine type of: " + t + " at position: " + position;
			memory.setByte(position, TypeReference.DELETED.getType());
		}
	}

	@Override
	public void commit() throws IOException {

	}

	@Override
	public void rollback() throws UnsupportedOperationException {

	}

	@Override
	public boolean isClosed() throws IOException {
		return closed.get();
	}

	@Override
	public void lock(long position, int size) {

	}

	@Override
	public void unlock(long position, int size) {

	}

	@Override
	public boolean isAppendOnly() {
		return false;
	}

	@Override
	public void close() throws IOException {
		closed.set(true);
		memory.free();
	}

	private static class ByteBufferInputStream extends InputStream {

		@Override
		public int read() throws IOException {
			return 0;
		}
	}

	private static class ByteBufferOutputStream extends OutputStream {

		@Override
		public void write(int b) throws IOException {

		}
	}

	private static class MemoryHeader extends Header {

		public MemoryHeader(int blockSize, String store) {
			super(blockSize, store);
		}

	}
}
