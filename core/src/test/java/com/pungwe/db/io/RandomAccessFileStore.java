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

import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.types.Header;

import java.io.*;
import java.nio.channels.FileChannel;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 29/09/2014.
 *
 * @Deprecated Focus will shift to a proper file store...
 */
@Deprecated
public class RandomAccessFileStore implements Store {

	private LinkedBlockingDeque<QueuedEntry> queue = new LinkedBlockingDeque<>();
	private long currentPosition;
	private final RandomAccessFile file;
	private final FileChannel fileChannel;
	private final FileDescriptor fd;
	private volatile boolean closed = false;

	// Next position
	final AtomicLong nextPosition;

	public RandomAccessFileStore(File file) throws IOException {
		// Open the file in question...
		this.file = new RandomAccessFile(file, "rw");
		this.fileChannel = this.file.getChannel();
		this.fd = this.file.getFD();
		this.nextPosition = new AtomicLong(this.getEndOfFile());
	}

	@Override
	public synchronized <T> long put(T value, Serializer<T> serializer) throws IOException {
		if (isClosed()) {
			throw new IOException("File has been closed");
		}
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bytes);
		serializer.serialize(out, value);
		out.flush();
		byte[] buf = bytes.toByteArray();
		long position = nextPosition.getAndAdd(buf.length);
		if (queue.remainingCapacity() == 0) {
			// flush
			flushQueue();
		}
		try {
			queue.put(new QueuedEntry(position, buf));
		} catch (InterruptedException ex) {
			throw new IOException(ex);
		}
		return position;
	}

	@Override
	public synchronized <T> T get(long position, Serializer<T> serializer) throws IOException {
		if (isClosed()) {
			throw new IOException("File has been closed");
		}
		if (!queue.isEmpty()) {
			Iterator<QueuedEntry> it = queue.descendingIterator();
			while (it.hasNext()) {
				QueuedEntry entry = it.next();
				if (entry.pointer == position) {
					return serializer.deserialize(new DataInputStream(new ByteArrayInputStream(entry.getBuffer())));
				}
			}
		}
		this.file.seek(position);
		return serializer.deserialize(file);
	}

	private void flushQueue() throws IOException {
		List<QueuedEntry> entries = new LinkedList<QueuedEntry>();
		int count = queue.drainTo(entries);
		for (QueuedEntry entry : entries) {
			this.file.seek(entry.pointer);
			this.file.write(entry.getBuffer());
		}
	}

	@Override
	public <T> long update(long position, T value, Serializer<T> serializer) throws IOException {
		return put(value, serializer);
	}

	@Override
	public Header getHeader() {
		return null;
	}

	@Override
	public synchronized void commit() throws IOException {

	}

	@Override
	public synchronized void rollback() throws UnsupportedOperationException {
		// Do nothing...
	}

	@Override
	public void remove(long position) {
		// Do nothing
	}

	@Override
	public synchronized boolean isClosed() throws IOException {
		return !file.getChannel().isOpen();
	}

	@Override
	public synchronized void close() throws IOException {
		flushQueue();
		System.out.println("File is: " + (this.file.length() / 1024d / 1024d) + "MB");
		closed = true;
		this.file.close();
	}

	private long preallocate(int size) throws IOException {
		this.file.setLength(this.file.length() + size);
		return this.file.length();
	}

	public long getEndOfFile() throws IOException {
		this.file.seek(this.file.length());
		return this.file.length();
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

	private class QueuedEntry {

		private QueuedEntry(long pointer, byte[] buffer) {
			this.pointer = pointer;
			this.buffer = buffer;
		}

		private long pointer;
		private byte[] buffer;

		public long getPointer() {
			return pointer;
		}

		public void setPointer(long pointer) {
			this.pointer = pointer;
		}

		public byte[] getBuffer() {
			return buffer;
		}

		public void setBuffer(byte[] buffer) {
			this.buffer = buffer;
		}
	}
}
