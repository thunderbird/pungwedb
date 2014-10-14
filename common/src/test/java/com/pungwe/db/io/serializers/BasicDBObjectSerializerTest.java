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
package com.pungwe.db.io.serializers;

import com.pungwe.db.types.BasicDBObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.*;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Created by ian on 07/09/2014.
 */
public class BasicDBObjectSerializerTest {


	@Before
	public void setup() {

	}

	@Test
	public void testSerializeDBObject() throws Exception {
		BasicDBObject object = new BasicDBObject();
		object.put("_id", UUID.randomUUID().toString());
		object.put("stringValue", "value");

		BasicDBObjectSerializer s = new BasicDBObjectSerializer();
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		s.serialize(new DataOutputStream(out), object);

		BasicDBObject result = s.deserialize(new DataInputStream(new ByteArrayInputStream(out.toByteArray())));

		assertEquals(object.get("_id"), result.get("_id"));
		assertEquals(object.get("stringValue"), result.get("stringValue"));
	}


	@After
	public void tearDown() {

	}
}
