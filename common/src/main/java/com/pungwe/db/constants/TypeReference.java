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
package com.pungwe.db.constants;

import org.joda.time.DateTime;

import java.util.*;

/**
 * Created by ian on 31/07/2014.
 */
public enum TypeReference {

	NULL((byte)'N'), BOOLEAN((byte)'b'), NUMBER((byte)'I'), BINARY((byte)'B'), DECIMAL((byte)'D'),
		STRING((byte)'S'), TIMESTAMP((byte)'Z'), ARRAY((byte)'A'), OBJECT((byte)'O'), ENTRY((byte)'E'),
		INDEX((byte)'X'), HEADER((byte)'H'), POINTER((byte)'P'), DELETED((byte)'x'), COMPRESSED((byte)'c');

	private byte type;

	private TypeReference(byte type) {
		this.type = type;
	}

	public byte getType() {
		return this.type;
	}

	public static TypeReference fromType(byte type) {
		TypeReference ref = null;
		switch (type) {
			case 'c':
				return COMPRESSED;
			case 'N':
				return NULL;
			case 'b':
				return BOOLEAN;
			case 'I':
				return NUMBER;
			case 'B':
				return BINARY;
			case 'D':
				return DECIMAL;
			case 'S':
				return STRING;
			case 'Z':
				return TIMESTAMP;
			case 'A':
				return ARRAY;
			case 'O':
				return OBJECT;
			case 'E':
				return ENTRY;
			case 'X':
				return INDEX;
			case 'H':
				return HEADER;
			case 'P':
				return POINTER;
			case 'x':
				return DELETED;
		}
		return null;
	}

	public static TypeReference forClass(Class<?> type) {
		if (type == null) {
			return NULL;
		} else if (byte[].class.isAssignableFrom(type)|| Byte[].class.isAssignableFrom(type)) {
			return BINARY;
		} else if (type.isArray()) {
			return ARRAY;
		} else if (String.class.isAssignableFrom(type)) {
			return STRING;
		} else if (Number.class.isAssignableFrom(type)) { // Process all numbers
			return NUMBER;
		} else if (Date.class.isAssignableFrom(type)) {
			return TIMESTAMP;
		} else if (Calendar.class.isAssignableFrom(type)) {
			return TIMESTAMP;
		} else if (DateTime.class.isAssignableFrom(type)) {
			return TIMESTAMP;
		} else if (Map.class.isAssignableFrom(type)) {
			return OBJECT;
		} else if (Iterable.class.isAssignableFrom(type)) {
			return ARRAY;
		} else if (Boolean.class.isAssignableFrom(type)) {
			return BOOLEAN;
		} else {
			throw new IllegalArgumentException("Object is not one the valid type references...");
		}
	}
}
