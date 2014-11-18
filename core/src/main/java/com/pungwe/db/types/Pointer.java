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
import com.pungwe.db.io.serializers.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ian on 04/11/2014.
 */
public class Pointer {

	final long pointer;

	public Pointer(long pointer) {
		this.pointer = pointer;
	}

	@Override
	public boolean equals(Object o) {
		throw new IllegalAccessError();
	}

	@Override
	public int hashCode() {
		throw new IllegalAccessError();
	}

	public long getPointer() {
		return pointer;
	}

	@Override
	public String toString() {
		return "Pointer{" +
				"pointer=" + pointer +
				'}';
	}
}
