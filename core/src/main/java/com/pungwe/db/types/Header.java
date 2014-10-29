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

import com.pungwe.db.io.Store;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by ian on 28/10/2014.
 */
public abstract class Header {

	protected final String store;
	protected int blockSize;
	protected final AtomicLong nextPosition = new AtomicLong();
	protected long metaData;

	public Header(int blockSize, String store) {
		this.blockSize = blockSize;
		this.store = store;
	}

	public Header(int blockSize, long currentPosition, String store) {
		this.blockSize = blockSize;
		setNextPosition(currentPosition);
		this.store = store;
	}

	public int getBlockSize() {
		return blockSize;
	}

	public void setBlockSize(int blockSize) {
		this.blockSize = blockSize;
	}

	public long getNextPosition() {
		return nextPosition.longValue();
	}

	protected synchronized void setNextPosition(long position) {
		nextPosition.set(position);
	}

	/**
	 * Store type information
	 * @return
	 */
	public String getStore() {
		return store;
	}

	/**
	 * Points to the structure that contains information about each collection / bucket stored in the file
	 * @return the position of the index.
	 */
	public long getMetaData() {
		return metaData;
	}

	public void setMetaData(long metaData) {
		this.metaData = metaData;
	}
}