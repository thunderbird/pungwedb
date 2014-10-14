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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 *
 * Created by ian on 08/09/2014.
 *
 * Serializer wraps reads and writers. The idea behind this is to simply provide a common method for serializing different
 * types of objects, such as a btree key, map node or record.
 *
 */
public interface Serializer<T> {

	/**
	 * Serialize a value into it's native format
	 * @param out Where the serialized value is written
	 * @param value The value to be serialized
	 * @throws IOException if an error occurs when writing the value
	 */
	void serialize(DataOutput out, T value) throws IOException;

	/**
	 * Reads a value from it's native form and converts it into
	 * @param in
	 * @return
	 * @throws IOException
	 */
	T deserialize(DataInput in) throws IOException;

	TypeReference getTypeReference();

}
