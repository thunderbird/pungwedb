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

import java.io.Serializable;
import java.util.*;

/**
 * Created by ian on 31/07/2014.
 * <p/>
 * DBObject contains a document. This object is essentially a linked map of entries in order of the keys placed into it.
 * <p/>
 * First 13 Bytes of the document are the record indicator [R], the size of the document and the number of keys.
 * <p/>
 * This object is not concerned with size, other than it needs to keep it up to date
 * <p/>
 * [E] = Start of Entry
 * <p/>
 * [1 Byte][4 Bytes][8 Bytes][n Bytes][1 Byte][4 Bytes][n Bytes]
 * <p/>
 * [R][COLLECTION][NUMBER_OF_KEYS]
 * [K][TIMESTAMP][Key][TYPE][LENGTH?][value]
 */
public class BasicDBObject extends AbstractDBObject implements Serializable {

	private boolean dirty = false;

	protected final Set<Entry<String, Object>> entries = new LinkedHashSet<Entry<String, Object>>();

	public BasicDBObject() {
	}

	public BasicDBObject(Map<? extends String, ?> m) {
		putAll(m);
	}

	@Override
	public Object put(String key, Object value) {
		for (Entry e : entries) {
			BasicNode n = (BasicNode)e;
			if (n.getKey().equals(key)) {
				n.setTimestamp(System.currentTimeMillis());
				n.setValue(value);
				dirty = true;
				return value;
			}
		}
		BasicNode n = new BasicNode(key, System.currentTimeMillis(), value);
		entries.add(n);
		return n.getValue();
	}

	@Override
	public Set<Entry<String, Object>> entrySet() {
		return entries;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		BasicDBObject dbObject = (BasicDBObject) o;

		if (!entries.equals(dbObject.entries)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		return entries.hashCode();
	}

	/**
	 * Specifies if this map has changed...
	 *
	 * @return
	 */
	public boolean isDirty() {
		return dirty;
	}


	public static final class BasicNode implements Node {

		private long timestamp;
		private String key;
		private Object value;

		public BasicNode(String key) {
			this.key = key;
		}

		public BasicNode(String key, long timestamp, Object value) {
			this.timestamp = timestamp;
			this.value = value;
			this.key = key;
		}

		@Override
		public void setTimestamp(long timestamp) {
			this.timestamp = timestamp;
		}

		public void setKey(String key) {
			this.key = key;
		}

		@Override
		public long getTimestamp() {
			return timestamp;
		}

		@Override
		public String getKey() {
			return key;
		}

		@Override
		public Object getValue() {
			return value;
		}

		@Override
		public Object setValue(Object value) {
			this.value = value;
			return value; // do nothing
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;

			BasicNode node = (BasicNode) o;

			if (!key.equals(node.key)) return false;

			return true;
		}

		@Override
		public int hashCode() {
			return key.hashCode();
		}
	}

}
