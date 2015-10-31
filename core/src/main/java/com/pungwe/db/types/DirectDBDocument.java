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
import com.pungwe.db.io.FastByteArrayOutputStream;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.serializers.Serializers;
import com.pungwe.db.io.store.DirectStore;
import com.pungwe.db.io.store.Store;
import com.pungwe.db.io.volume.MemoryVolume;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 10/04/2015.
 */
public class DirectDBDocument extends AbstractDBDocument {

	private final Store store;
	private final AtomicLong lastKey = new AtomicLong();

	public DirectDBDocument() throws IOException {
		store = new DirectStore(new MemoryVolume(false), new MemoryVolume(false));
	}

	@Override
	public Object put(String key, Object value) {
		return null;
	}

	@Override
	public Set<Entry<String, Object>> entrySet() {
		return null;
	}

	private static class DBDocumentNode implements Node {

		private long timestamp;
		private String key;
		private Object value;
		private long previous;

		@Override
		public void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}

		@Override
		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public String getKey() {
			return this.key;
		}

		public void setKey(String key) {
			this.key = key;
		}

		@Override
		public Object getValue() {
			return this.value;
		}

		@Override
		public Object setValue(Object value) {
			this.value = value;
			return value;
		}
	}

	private static class DBDocumentNodeSerializer implements Serializer<DBDocumentNode> {

		@Override
		public void serialize(DataOutput out, DBDocumentNode value) throws IOException {
			long timestamp = value.timestamp;
			String key = value.getKey();
			Object v = value.getValue();

			FastByteArrayOutputStream outputStream = new FastByteArrayOutputStream();
			DataOutputStream dataOut = new DataOutputStream(outputStream);

			dataOut.writeLong(timestamp);
			dataOut.writeByte(TypeReference.STRING.getType());
			Serializer<String> keySerializer = new Serializers.STRING();
			keySerializer.serialize(dataOut, key);
			Serializer<Object> valueSerializer = Serializers.serializerForType(v);
			dataOut.writeByte(valueSerializer.getTypeReference().getType());
			valueSerializer.serialize(dataOut, v);

			byte[] bytes = outputStream.toByteArray();
			out.writeInt(bytes.length);
			out.write(bytes);
		}

		@Override
		public DBDocumentNode deserialize(DataInput in) throws IOException {
			return null;
		}

		@Override
		public TypeReference getTypeReference() {
			return null;
		}
	}
}
