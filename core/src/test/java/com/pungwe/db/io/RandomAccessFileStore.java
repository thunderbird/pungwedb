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

import com.pungwe.db.io.Store;
import com.pungwe.db.io.serializers.Serializer;

import java.io.*;
import java.nio.channels.FileChannel;

/**
 * Created by ian on 29/09/2014.
 */
public class RandomAccessFileStore implements Store {

	private long currentPosition;
	private final RandomAccessFile file;
	private final FileChannel fileChannel;
	private final FileDescriptor fd;

	public RandomAccessFileStore(File file) throws IOException {
		// Open the file in question...
		this.file = new RandomAccessFile(file, "rw");
		this.fileChannel = this.file.getChannel();
		this.fd = this.file.getFD();

	}

	@Override
	public synchronized <T> long put(T value, Serializer<T> serializer) throws IOException {
		long position = getEndOfFile();
		serializer.serialize(file, value);
		return position;
	}

	@Override
	public synchronized <T> T get(long position, Serializer<T> serializer) throws IOException {
		this.file.seek(position);
		return serializer.deserialize(file);
	}

	@Override
	public <T> long update(long position, T value, Serializer<T> serializer) throws IOException {
		return put(value, serializer);
	}

	@Override
	public synchronized void commit() {
		// Write the file header and flush
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
}
