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
import com.pungwe.db.types.Header;

import java.io.*;
import java.nio.BufferOverflowException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO: Add expansion as we can very quickly run out of space.
 * Created by ian on 28/10/2014.
 */
public class MemoryMappedFileStore implements Store {

	// We don't want anymore than 2GB per segment...
	private static final int TWOGIG = Integer.MAX_VALUE;
	private static final int PAGE_SIZE = 4096; // 4KB blocks of data.

	private List<MappedByteBuffer> segments = new ArrayList<>();

	private RandomAccessFile file;
	private long length;
	private long increment;
	private final long maxLength;
	private final MemoryMappedFileHeader header;

	public MemoryMappedFileStore(File file, long initialSize, long maxFileSize) throws IOException {
		this.file = new RandomAccessFile(file, "rw");
		length = initialSize;
		increment = initialSize;
		maxLength = maxFileSize;

		boolean newHeader = false;
		if (this.file.length() == 0) {
			this.file.setLength(initialSize);
			newHeader = true;
		} else {
			// Find the end of the file. We are a top reader, so the header should be there...
			if (initialSize == -1) {
				initialSize = this.file.length();
			}
		}
		map(initialSize);

		if (newHeader) {
			this.header = new MemoryMappedFileHeader(PAGE_SIZE, PAGE_SIZE);
			writeHeader();
		} else {
			this.header = (MemoryMappedFileHeader) findHeader();
		}
	}

	private void map(long newLength) throws IOException {
		segments = new ArrayList<>();
		long segCount = (long) Math.ceil((double) newLength / TWOGIG);
		if (segCount > TWOGIG) {
			throw new ArithmeticException("Requested File Size is too large");
		}
		long countDown = newLength;
		long from = 0;
		FileChannel channel = this.file.getChannel();
		while (countDown > 0) {
			long len = Math.min(TWOGIG, countDown);
			MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, from, len);
			segments.add(buffer);
			from += len;
			countDown -= len;
		}
	}

	private void expand() throws IOException {
		long newLength = this.file.length() + increment;
		if (maxLength > 0 && newLength > maxLength) {
			throw new IOException("Cannot expand beyond max size of: " + ((double)maxLength / 1024 / 1024) + "GB");
		}
		this.file.setLength(newLength);
		map(newLength);
	}

	private Header findHeader() throws IOException {
		long current = 0;
		while (current < file.length()) {
			byte[] buffer = new byte[PAGE_SIZE];
			this.file.read(buffer);
			byte firstByte = buffer[0];
			if (firstByte == TypeReference.HEADER.getType()) {
				ByteArrayInputStream bytes = new ByteArrayInputStream(buffer);
				DataInputStream in = new DataInputStream(bytes);
				in.skip(5);
				return new MemoryMappedFileSerializer().deserialize(in);
			}
			current += PAGE_SIZE;
		}
		throw new IOException("Could not find file header. File could be wrong or corrupt");
	}

	private synchronized void writeHeader() throws IOException {
		// Get the first segment and write the header
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bytes);
		new MemoryMappedFileSerializer().serialize(out, this.header);
		// We must not exceed PAGE_SIZE
		byte[] data = bytes.toByteArray();
		assert data.length < PAGE_SIZE - 5 : "Header is larger than a block...";
		// Get the first segment
		MappedByteBuffer segment = null;
		if (segments.size() > 0) {
			segment = segments.get(0);
		}
		segment.position(0);
		segment.put(TypeReference.HEADER.getType());
		segment.putInt(data.length);
		segment.put(data);
		if (data.length < (PAGE_SIZE - 5)) {
			segment.put(new byte[(PAGE_SIZE - 5) - data.length]);
		}
	}

	@Override
	public <T> long put(T value, Serializer<T> serializer) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bytes);
		serializer.serialize(out, value);
		byte[] data = bytes.toByteArray();
		int pages = (int) Math.ceil((double) data.length / header.getBlockSize());
		long position = getHeader().getNextPosition(pages * header.getBlockSize());
		write(position, data, TypeReference.OBJECT);
		// Update the header
		synchronized (header) {
			writeHeader();
		}
		// return the position

		return position;
	}

	@Override
	public <T> T get(long position, Serializer<T> serializer) throws IOException {

		// We need to read the first few bytes
		byte[] data = read(position);
		ByteArrayInputStream is = new ByteArrayInputStream(data);
		DataInputStream in = new DataInputStream(is);
		T value = serializer.deserialize(in);

		return value;
	}

	@Override
	public <T> long update(long position, T value, Serializer<T> serializer) throws IOException {

		double a = position;
		double b = TWOGIG;
		long whichSegment = (long) Math.floor(a / b);
		long withinSegment = position - whichSegment * TWOGIG;

		ByteBuffer readBuffer = segments.get((int) whichSegment).asReadOnlyBuffer();
		readBuffer.position((int) withinSegment);
		byte type = readBuffer.get();
		assert TypeReference.fromType(type) != null;
		int size = readBuffer.getInt();
		int origPageSize = (int) Math.ceil(((double)size + 5) / header.getBlockSize());

		ByteArrayOutputStream bytesOut = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bytesOut);
		serializer.serialize(out, value);
		byte[] bytes = bytesOut.toByteArray();
		int newPageSize = (int)Math.ceil(((double)bytes.length + 5) / header.getBlockSize());

		if (newPageSize > origPageSize) {
			position = getHeader().getNextPosition(newPageSize * header.getBlockSize());
		}

		write(position, bytes, TypeReference.OBJECT);

		synchronized (header) {
			writeHeader();
		}

		return position;
	}

	public byte[] read(long offset) throws IOException {

		double a = offset;
		double b = TWOGIG;
		long whichSegment = (long) Math.floor(a / b);
		long withinSegment = offset - whichSegment * TWOGIG;

		// Get the segment relevant segment
		ByteBuffer readBuffer = segments.get((int) whichSegment).asReadOnlyBuffer();
		readBuffer.position((int) withinSegment);
		byte t = readBuffer.get();
		int size = readBuffer.getInt(); // Size of thing we're reading
		assert TypeReference.fromType(t) != null : "Cannot determine type: " + t + " segment: " + whichSegment + " at " + withinSegment + " size: " + size;

		// Create a byte array for this
		byte[] data = new byte[size];
		try {
			if (TWOGIG - withinSegment > data.length) {
				readBuffer.position((int) withinSegment + 5);
				readBuffer.get(data, 0, data.length);
				return data;
			} else {
				int l1 = (int) (TWOGIG - withinSegment + 5);
				int l2 = (int) data.length - l1;
				readBuffer.position((int) withinSegment + 5);
				readBuffer.get(data, 0, l1);

				readBuffer = segments.get((int) whichSegment + 1).asReadOnlyBuffer();
				readBuffer.position(0);
				try {
					readBuffer.get(data, l1, l2);
				} catch (BufferUnderflowException ex) {
					throw ex;
				}
				return data;
			}
		} catch (IndexOutOfBoundsException ex) {
			throw new IOException("Out of bounds");
		}
	}

	public void write(long offSet, byte[] src, TypeReference type) throws IOException {
		// Quick and dirty but will go wrong for massive numbers
		double a = offSet;
		double b = TWOGIG;
		long whichChunk = (long) Math.floor(a / b);
		long withinChunk = offSet - whichChunk * TWOGIG;

		// Data does not straddle two chunks
		try {
			if (src.length + 1 + offSet > this.file.length()) {
				expand();
			}
			ByteBuffer segment = segments.get((int) whichChunk);
			long remaining = segment.capacity() - withinChunk;
			if (remaining > src.length + 1) {
				// Allows free threading
				ByteBuffer writeBuffer = segment.duplicate();
				writeBuffer.position((int) withinChunk);
				writeBuffer.put(type.getType());
				writeBuffer.putInt(src.length);
				writeBuffer.put(src, 0, src.length);
			} else {
				int l1 = (int) (segment.capacity() - (withinChunk + 1));
				int l2 = (int) (src.length - 1) - l1;

				// Allows free threading
				ByteBuffer writeBuffer = segment.duplicate();
				writeBuffer.position((int) withinChunk);
				writeBuffer.put(type.getType());
				writeBuffer.putInt(src.length);
				writeBuffer.put(src, 0, l1);

				segment = segments.get((int) whichChunk + 1);
				writeBuffer = segment.duplicate();
				writeBuffer.position(0);
				writeBuffer.put(src, l1, l2);

			}
		} catch (BufferOverflowException ex) {
			throw ex;
		}
	}

	@Override
	public Header getHeader() {
		return header;
	}

	@Override
	public void remove(long position) {
		double a = position;
		double b = TWOGIG;
		long whichSegment = (long) Math.floor(a / b);
		long withinSegment = position - whichSegment * TWOGIG;

		// Get the segment relevant segment
		ByteBuffer readBuffer = segments.get((int) whichSegment).asReadOnlyBuffer();
		readBuffer.position((int) withinSegment);
		byte t = readBuffer.get();
		assert TypeReference.fromType(t) != null : "Cannot determine type";
		ByteBuffer writeBuffer = segments.get((int) whichSegment).duplicate();
		writeBuffer.position((int)withinSegment);
		writeBuffer.put(TypeReference.DELETED.getType());

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
		file.close();
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
			out.writeUTF(value.getStore());
			out.writeInt(value.getBlockSize());
			out.writeLong(value.getPosition());
			out.writeLong(value.getMetaData());
		}

		@Override
		public MemoryMappedFileHeader deserialize(DataInput in) throws IOException {
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
