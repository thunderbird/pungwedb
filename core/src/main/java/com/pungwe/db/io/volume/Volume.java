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
public interface Volume extends /*DataInput, DataOutput,*/ Closeable {

	byte[] CLEAR = new byte[1024];

	//void seek(long position) throws IOException;
	//long getPosition() throws IOException;
	long getLength() throws IOException;
	long getPositionLimit();
	void ensureAvailable(long offset) throws IOException;
	boolean isClosed();
	void clear(long startOffset, long endOffset) throws IOException;

//	/*
//		Input
//	*/
//	@Override
//	default String readUTF() throws IOException {
//		return getInput(getPosition()).readUTF();
//	}
//
//	@Override
//	default int skipBytes(int n) throws IOException {
//		return getInput(getPosition()).skipBytes(n);
//	}
//
//	@Override
//	default boolean readBoolean() throws IOException {
//		return getInput(getPosition()).readBoolean();
//	}
//
//	@Override
//	default byte readByte() throws IOException {
//		return getInput(getPosition()).readByte();
//	}
//
//	@Override
//	default int readUnsignedByte() throws IOException {
//		return getInput(getPosition()).readUnsignedByte();
//	}
//
//	@Override
//	default short readShort() throws IOException {
//		return getInput(getPosition()).readShort();
//	}
//
//	@Override
//	default int readUnsignedShort() throws IOException {
//		return getInput(getPosition()).readUnsignedByte();
//	}
//
//	@Override
//	default char readChar() throws IOException {
//		return getInput(getPosition()).readChar();
//	}
//
//	@Override
//	default int readInt() throws IOException {
//		return getInput(getPosition()).readInt();
//	}
//
//	@Override
//	default long readLong() throws IOException {
//		return getInput(getPosition()).readLong();
//	}
//
//	@Override
//	default float readFloat() throws IOException {
//		return getInput(getPosition()).readFloat();
//	}
//
//	@Override
//	default double readDouble() throws IOException {
//		return getInput(getPosition()).readDouble();
//	}
//
//	@Override
//	default String readLine() throws IOException {
//		return getInput(getPosition()).readLine();
//	}
//
//	@Override
//	default void readFully(byte[] b, int off, int len) throws IOException {
//		getInput(getPosition()).readFully(b, off, len);
//	}
//
//	@Override
//	default void readFully(byte[] b) throws IOException {
//		readFully(b, 0, b.length);
//	}
//
//	/*
//	Output
//	 */
//	@Override
//	default void write(int b) throws IOException {
//		getOutput(getPosition()).write(b);
//	}
//
//	@Override
//	default void write(byte[] b) throws IOException {
//		getOutput(getPosition()).write(b);
//	}
//
//	@Override
//	default void write(byte[] b, int off, int len) throws IOException {
//		getOutput(getPosition()).write(b, off, len);
//	}
//
//	@Override
//	default void writeBoolean(boolean v) throws IOException {
//		getOutput(getPosition()).writeBoolean(v);
//	}
//
//	@Override
//	default void writeByte(int v) throws IOException {
//		getOutput(getPosition()).writeByte(v);
//	}
//
//	@Override
//	default void writeShort(int v) throws IOException {
//		getOutput(getPosition()).writeShort(v);
//	}
//
//	@Override
//	default void writeChar(int v) throws IOException {
//		getOutput(getPosition()).writeChar(v);
//	}
//
//	@Override
//	default void writeInt(int v) throws IOException {
//		getOutput(getPosition()).writeInt(v);
//	}
//
//	@Override
//	default void writeLong(long v) throws IOException {
//		getOutput(getPosition()).writeLong(v);
//	}
//
//	@Override
//	default void writeFloat(float v) throws IOException {
//		getOutput(getPosition()).writeFloat(v);
//	}
//
//	@Override
//	default void writeDouble(double v) throws IOException {
//		getOutput(getPosition()).writeDouble(v);
//	}
//
//	@Override
//	default void writeBytes(String s) throws IOException {
//		getOutput(getPosition()).writeBytes(s);
//	}
//
//	@Override
//	default void writeChars(String s) throws IOException {
//		getOutput(getPosition()).writeChars(s);
//	}
//
//	@Override
//	default void writeUTF(String s) throws IOException {
//		getOutput(getPosition()).writeUTF(s);
//	}

	DataInput getInput(long offset);
	DataOutput getOutput(long offset);
}
