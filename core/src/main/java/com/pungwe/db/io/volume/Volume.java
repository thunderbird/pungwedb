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
package com.pungwe.db.io.volume;

import java.io.Closeable;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ian on 04/02/2015.
 */
public interface Volume extends Closeable {

	void seek(long position) throws IOException;
	long getPosition();
	long getPositionLimit();
	void mark();

	long readLong();
	int readInt();
	byte readByte();
	char readChar();
	String readUTF();

	default void read(byte[] b) {
		read(b, 0, b.length);
	}

	default void read(byte[] b, int off, int len) {
		read(getPosition(), b, off, len);
	}

	void read(long offset, byte[] b, int off, int len);

	DataInput getInput();
	DataOutput getOutput();

}
