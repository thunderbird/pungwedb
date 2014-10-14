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
import com.pungwe.db.types.BasicDBObject;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.*;
import java.util.*;

/**
 * Created by ian on 01/08/2014.
 */
public class DBObjectReader {

	public static DBObject read(byte[] buf) throws IOException {
		return read(new DataInputStream(new ByteArrayInputStream(buf)));
	}

	public static DBObject read(DataInput input) throws IOException {
		DBObject object = new DBObject();
		// We don't really care about object length as we just iterate keys
		int keysRead = 0;
		int keys = input.readInt(); // First Integer is the key size of the document

		for (int i = 0; i < keys; i++) {
			// Ensure we are reading a document the first byte of each key should be "K". The document should comprise of keys
			if (input.readByte() != TypeReference.ENTRY.getType()) {
				throw new IllegalArgumentException("Provided data input is not a document");
			}
			// The next entry on a key should be the key timestamp
			long timestamp = input.readLong();
			// Get the key name
			String key = input.readUTF();
			// Read the key value
			Object value = readObject(input);

			// Create the object entry
			BasicDBObject.BasicNode node = new BasicDBObject.BasicNode(key, timestamp, value);
			object.addEntry(node);
		}

		return object;
	}

	private static DateTime readTimestamp(DataInput input) throws IOException {
		String dateString = input.readUTF();
		DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
		return fmt.parseDateTime(dateString);
	}

	private static Object readObject(DataInput input) throws IOException {
		byte t = input.readByte();
		TypeReference type = TypeReference.fromType(t);
		switch (type) {
			case NULL:
				return null;
			case TRUE:
				return true;
			case FALSE:
				return false;
			case NUMBER:
				return input.readLong();
			case DECIMAL:
				return input.readDouble();
			case BINARY: {
				// Check for length
				int length = input.readInt();
				byte[] byteArray = new byte[length];
				input.readFully(byteArray);
				return byteArray;
			}
			case OBJECT: {
				return read(input);
			}
			case TIMESTAMP: {
				return readTimestamp(input);
			}
			case STRING: {
				return input.readUTF();
			}
			case ARRAY: {
				int size = input.readInt();
				List array = new ArrayList(size);
				for (int i = 0; i < size; i++) {
					Object value = readObject(input);
					array.add(value);
				}
				return array;
			}
		}
		return null;
	}

	private static class DBObject extends BasicDBObject {

		public void addEntry(BasicNode n) {
			this.entries.add(n);
		}
	}
}
