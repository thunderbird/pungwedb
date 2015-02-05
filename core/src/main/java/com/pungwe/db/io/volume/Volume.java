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
public interface Volume extends DataInput, DataOutput, Closeable {

	void seek(long position) throws IOException;
	long getPosition() throws IOException;
	long getLength() throws IOException;
	long getPositionLimit();
	void mark() throws IOException;
	void ensureAvailable(long offset) throws IOException;
	boolean isClosed();

	/*
		Input
	*/
	@Override
	default String readUTF() throws IOException {
		return getInput().readUTF();
	}

	@Override
	default int skipBytes(int n) throws IOException {
		return getInput().skipBytes(n);
	}

	@Override
	default boolean readBoolean() throws IOException {
		return getInput().readBoolean();
	}

	@Override
	default byte readByte() throws IOException {
		return getInput().readByte();
	}

	@Override
	default int readUnsignedByte() throws IOException {
		return getInput().readUnsignedByte();
	}

	@Override
	default short readShort() throws IOException {
		return getInput().readShort();
	}

	@Override
	default int readUnsignedShort() throws IOException {
		return getInput().readUnsignedByte();
	}

	@Override
	default char readChar() throws IOException {
		return getInput().readChar();
	}

	@Override
	default int readInt() throws IOException {
		return getInput().readInt();
	}

	@Override
	default long readLong() throws IOException {
		return getInput().readLong();
	}

	@Override
	default float readFloat() throws IOException {
		return getInput().readFloat();
	}

	@Override
	default double readDouble() throws IOException {
		return getInput().readDouble();
	}

	@Override
	default String readLine() throws IOException {
		return getInput().readLine();
	}

	@Override
	default void readFully(byte[] b, int off, int len) throws IOException {
		getInput().readFully(b, off, len);
	}

	@Override
	default void readFully(byte[] b) throws IOException {
		readFully(b, 0, b.length);
	}

	/*
	Output
	 */
	@Override
	default void write(int b) throws IOException {
		getOutput().write(b);
	}

	@Override
	default void write(byte[] b) throws IOException {
		getOutput().write(b);
	}

	@Override
	default void write(byte[] b, int off, int len) throws IOException {
		getOutput().write(b, off, len);
	}

	@Override
	default void writeBoolean(boolean v) throws IOException {
		getOutput().writeBoolean(v);
	}

	@Override
	default void writeByte(int v) throws IOException {
		getOutput().writeByte(v);
	}

	@Override
	default void writeShort(int v) throws IOException {
		getOutput().writeShort(v);
	}

	@Override
	default void writeChar(int v) throws IOException {
		getOutput().writeChar(v);
	}

	@Override
	default void writeInt(int v) throws IOException {
		getOutput().writeInt(v);
	}

	@Override
	default void writeLong(long v) throws IOException {
		getOutput().writeLong(v);
	}

	@Override
	default void writeFloat(float v) throws IOException {
		getOutput().writeFloat(v);
	}

	@Override
	default void writeDouble(double v) throws IOException {
		getOutput().writeDouble(v);
	}

	@Override
	default void writeBytes(String s) throws IOException {
		getOutput().writeBytes(s);
	}

	@Override
	default void writeChars(String s) throws IOException {
		getOutput().writeChars(s);
	}

	@Override
	default void writeUTF(String s) throws IOException {
		getOutput().writeUTF(s);
	}

	DataInput getInput();
	DataOutput getOutput();
}
