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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by ian on 28/10/2014.
 */
public class MemoryMappedFileStore implements Store {

	// We don't want anymore than 2GB per segment...
	private static final int MAX_SEGMENT_SIZE = Integer.MAX_VALUE;

	private List<MappedByteBuffer> segments = new ArrayList<MappedByteBuffer>();

	@Override
	public <T> long put(T value, Serializer<T> serializer) throws IOException {
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
