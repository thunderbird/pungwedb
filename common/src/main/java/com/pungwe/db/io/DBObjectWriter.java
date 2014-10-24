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
import com.pungwe.db.types.DBObject;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 01/08/2014.
 */
public class DBObjectWriter {

	private DataOutput output;

	int keys = 0;
	int size = 0;

	private DBObjectWriter(DataOutput output) {
		this.output = output;
	}

	public static void write(DBObject object, DataOutput output) throws IOException {
		DBObjectWriter writer = new DBObjectWriter(output);
		output.writeInt(object.size()); // Insert key size as the first 4 bytes...
		Iterator<Map.Entry<String, Object>> iterator = object.entrySet().iterator();
		//writer.updateHeaders();
		while (iterator.hasNext()) {
			DBObject.Node n = (DBObject.Node)iterator.next();
			writer.writeEntry(n.getKey(), n.getTimestamp(), n.getValue());
		}

	}

	private void writeEntry(String key, long timestamp, Object value) throws IOException {
		
		output.write(TypeReference.ENTRY.getType());
		output.writeLong(timestamp);
		output.writeUTF(key);
		writeObject(value);
	}

	private void writeObject(Object value) throws IOException {

		if (value == null) {
			output.write(TypeReference.NULL.getType());
		} else if (value instanceof String) {
			output.write(TypeReference.STRING.getType());
			output.writeUTF((String)value);
		} else if (value instanceof byte[] || value instanceof Byte[]) {
			output.write(TypeReference.BINARY.getType());
			output.writeInt(((byte[]) value).length);
			output.write((byte[]) value);
		} else if (value instanceof Number) { // Process all numbers
			writeNumber((Number) value);
		} else if (value instanceof Date) {
			// We want to write an appropriate date, so for now set this to null and
			// we will create a special date object to store all the correct and relevant time values.
			// It will probably need to use joda... But it will certainly be UTC based time stamps
			output.write(TypeReference.TIMESTAMP.getType());
			writeDate((Date) value);
		} else if (value instanceof Calendar) {
			output.write(TypeReference.TIMESTAMP.getType());
			writeDate((Calendar) value);
		} else if (value instanceof DateTime) {
			output.write(TypeReference.TIMESTAMP.getType());
			writeDate((DateTime) value);
		} else if (value instanceof Map) {
			output.write(TypeReference.OBJECT.getType());
			writeMap((Map) value);
		} else if (value instanceof Iterable) {
			output.write(TypeReference.ARRAY.getType());
			writeArray((Collection)value);
		} else if (value != null && value.getClass().isArray()) {
			output.write(TypeReference.ARRAY.getType());
			writeArray(Arrays.asList((Object[])value));
		} else if (value instanceof Boolean) {
			output.writeByte(TypeReference.BOOLEAN.getType());
			output.writeBoolean((Boolean) value);
		} else {
            throw new IllegalArgumentException("Cannot determine type");
        }
	}

	private void writeDate(Date date) throws IOException {
		Calendar c = Calendar.getInstance();
		c.setTime(date);
		writeDate(c);
	}

	private void writeDate(Calendar calendar) throws IOException {
		DateTime date = new DateTime(calendar);
		writeDate(date);
	}

	private void writeDate(DateTime date) throws IOException {
		DateTimeFormatter fmt = ISODateTimeFormat.dateTime();
		output.writeUTF(fmt.print(date));
	}

	private void writeArray(Collection value) throws IOException {
		Iterator it = value.iterator();
		// Create a writer (a new buffer)
        output.writeInt(value.size());
		while (it.hasNext()) {
			Object next = it.next();
			// Write value into new buffer
			writeObject(next);
		}
	}

	private void writeNumber(Number number) throws IOException {
		if (number instanceof Integer || number instanceof Short || number instanceof Byte ||
				number instanceof AtomicInteger || number instanceof Long || number instanceof AtomicLong) {
			output.write(TypeReference.NUMBER.getType());
			output.writeLong(number.longValue());
		} else if (number instanceof Float || number instanceof Double) {
			output.write(TypeReference.DECIMAL.getType());
			output.writeDouble(number.doubleValue());
		} else {
			throw new IllegalArgumentException("Cannot write type: " + number.getClass().getName());
		}
	}

	private void writeMap(Map map) throws IOException {
		//FastByteArrayOutputStream objectOutput = new FastByteArrayOutputStream();
		if (!(map instanceof BasicDBObject)) {
			write(new BasicDBObject(map), output);
		} else {
			write((BasicDBObject)map, output);
		}
	}

}
