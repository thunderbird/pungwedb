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

import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.io.DBObjectReader;
import com.pungwe.db.io.DBObjectWriter;
import com.pungwe.db.types.BasicDBObject;
import com.pungwe.db.types.DBObject;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;

/**
 * Created by ian on 07/09/2014.
 */
public class DBObjectSerializer implements Serializer<DBObject> {

	@Override
	public void serialize(DataOutput out, DBObject value) throws IOException {
		DBObjectWriter.write(value, out);
	}

	@Override
	public DBObject deserialize(DataInput in) throws IOException {
		return DBObjectReader.read(in);
	}

	@Override
	public TypeReference getTypeReference() {
		return TypeReference.OBJECT;
	}

}
