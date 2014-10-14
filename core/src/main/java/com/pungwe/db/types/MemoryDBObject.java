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

import com.pungwe.db.types.AbstractDBObject;
import com.pungwe.db.types.BasicDBObject;

import java.util.Map;
import java.util.Set;

/**
 * Created by ian on 04/10/2014.
 */
// Borrowed from Cassandra
public class MemoryDBObject extends AbstractDBObject {


	@Override
	public Object put(String key, Object value) {
		return null;
	}

	@Override
	public Set<Entry<String, Object>> entrySet() {
		return null;
	}

	@Override
	public boolean isDirty() {
		return false;
	}

	public static class MemoryNode implements Node {

		@Override
		public void setTimestamp(long timestamp) {

		}

		@Override
		public String getKey() {
			return null;
		}

		@Override
		public Object getValue() {
			return null;
		}

		@Override
		public Object setValue(Object value) {
			return null;
		}
	}
}
