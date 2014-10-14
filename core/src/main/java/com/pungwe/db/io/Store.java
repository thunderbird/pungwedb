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
// Borrowed from Cassandra
package com.pungwe.db.io;

import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.io.serializers.Serializer;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by ian on 08/09/2014.
 */
public interface Store extends Closeable {

	<T> long put(T value, Serializer<T> serializer) throws IOException;

	<T> T get(long position, Serializer<T> serializer) throws IOException;

	<T> long update(long position, T value, Serializer<T> serializer) throws IOException;

	void commit();

	void rollback() throws UnsupportedOperationException;

	public boolean isClosed();

	void lock(long position, int size);

	void unlock(long position, int size);
}
