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
import com.pungwe.db.types.DBObject;

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
	private final AtomicLong nextPosition = new AtomicLong();

	public MemoryMappedFileStore(File file, long initialSize, long maxFileSize) throws IOException {
		this.file = new RandomAccessFile(file, "rw");
		if (this.file.length() == 0) {
			this.file.setLength(initialSize);
			this.nextPosition.set(0); // Set to the top of the file
		} else {
			// Find the end of the file
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
}
