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
import com.pungwe.db.types.DBObject;
import com.pungwe.db.types.Header;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 28/10/2014.
 */
public class MemoryMappedFileStore implements Store {

	// We don't want anymore than 2GB per segment...
	private static final int MAX_SEGMENTS = Integer.MAX_VALUE;
	private static final int PAGE_SIZE = 4096; // 4KB blocks of data.

	private List<MappedByteBuffer> segments = new ArrayList<>();

	private RandomAccessFile file;
	private long length;
	private final long maxLength;
	private final MemoryMappedFileHeader header;

	public MemoryMappedFileStore(File file, long initialSize, long maxFileSize) throws IOException {
		this.file = new RandomAccessFile(file, "rw");
		if (this.file.length() == 0) {
			this.file.setLength(initialSize);
			this.header = new MemoryMappedFileHeader(PAGE_SIZE, 0);
		} else {
			// Find the end of the file. We are a top reader, so the header should be there...
			this.header = (MemoryMappedFileHeader)findHeader();
		}
		long segCount = initialSize / MAX_SEGMENTS;
		if (segCount > MAX_SEGMENTS) {
			throw new ArithmeticException("Requested File Size is too large");
		}
		length = initialSize;
		maxLength = maxFileSize;
		long countDown = initialSize;
		long from = 0;
		FileChannel channel = this.file.getChannel();
		while (countDown > 0) {
			long len = Math.min(MAX_SEGMENTS, countDown);
			MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, from, len);
			segments.add(buffer);
			from += len;
			countDown -= len;
		}
	}

	private Header findHeader() throws IOException {
		long current = 0;
		while (current < file.length()) {
			byte[] buffer = new byte[4096];
			this.file.read(buffer);
			byte firstByte = buffer[0];
			if (firstByte == TypeReference.HEADER.getType()) {
				return get(current, new MemoryMappedFileSerializer());
			}
			current += 4096;
		}
		throw new IOException("Could not find file header. File could be wrong or corrupt");
	}

	private void writeHeader() throws IOException {
		// Get the first segment and write the header
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bytes);
		new MemoryMappedFileSerializer().serialize(out, this.header);
		// We must not exceed PAGE_SIZE
		byte[] data = bytes.toByteArray();
		assert data.length < PAGE_SIZE : "Header is larger than a block...";
		// Get the first segment
		ByteBuffer segment = segments.get(0);
		ByteBuffer wrapped = ByteBuffer.wrap(data);
		wrapped.limit(PAGE_SIZE);
		segment.put(wrapped);
	}

	@Override
	public <T> long put(T value, Serializer<T> serializer) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bytes);

		return 0;
	}

	@Override
	public <T> T get(long position, Serializer<T> serializer) throws IOException {
		return null;
	}

	@Override
	public <T> long update(long position, T value, Serializer<T> serializer) throws IOException {
		return 0;
	}

	@Override
	public Header getHeader() {
		return null;
	}

	@Override
	public void remove(long position) {

	}

	@Override
	public void commit() throws IOException {

	}

	@Override
	public void rollback() throws UnsupportedOperationException {

	}

	@Override
	public boolean isClosed() throws IOException {
		return false;
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

	}

	private static class MemoryMappedFileHeader extends Header {

		public MemoryMappedFileHeader(int blockSize) {
			super(blockSize, MemoryMappedFileStore.class.getName());
		}

		public MemoryMappedFileHeader(int blockSize, long currentPosition) {
			super(blockSize, currentPosition, MemoryMappedFileStore.class.getName());
		}
	}

	private class MemoryMappedFileSerializer implements Serializer<MemoryMappedFileHeader> {

		@Override
		public void serialize(DataOutput out, MemoryMappedFileHeader value) throws IOException {
			out.writeByte(TypeReference.HEADER.getType());
			out.writeUTF(value.getStore());
			out.writeInt(value.getBlockSize());
			out.writeLong(value.getNextPosition());
			out.writeLong(value.getMetaData());
		}

		@Override
		public MemoryMappedFileHeader deserialize(DataInput in) throws IOException {
			byte type = in.readByte();
			String store = in.readUTF();
			assert store.equals(MemoryMappedFileStore.class.getName());
			int blockSize = in.readInt();
			long nextPosition = in.readLong();
			long metaData = in.readLong();
			MemoryMappedFileHeader header = new MemoryMappedFileHeader(blockSize, nextPosition);
			header.setMetaData(metaData);
			return header;
		}

		@Override
		public TypeReference getTypeReference() {
			return TypeReference.HEADER;
		}

	}
}
