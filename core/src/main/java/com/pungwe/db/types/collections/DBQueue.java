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
package com.pungwe.db.types.collections;

import java.util.AbstractQueue;
import java.util.Collection;
import java.util.Iterator;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * Created by ian on 17/03/2015.
 */
public final class DBQueue<E> extends AbstractQueue<E> implements BlockingQueue<E> {

	@Override
	public Iterator<E> iterator() {
		return null;
	}

	@Override
	public int size() {
		return 0;
	}

	@Override
	public void put(E e) throws InterruptedException {

	}

	@Override
	public boolean offer(E e, long timeout, TimeUnit unit) throws InterruptedException {
		return false;
	}

	@Override
	public E take() throws InterruptedException {
		return null;
	}

	@Override
	public E poll(long timeout, TimeUnit unit) throws InterruptedException {
		return null;
	}

	@Override
	public int remainingCapacity() {
		return 0;
	}

	@Override
	public int drainTo(Collection<? super E> c) {
		return 0;
	}

	@Override
	public int drainTo(Collection<? super E> c, int maxElements) {
		return 0;
	}

	@Override
	public boolean offer(E e) {
		return false;
	}

	@Override
	public E poll() {
		return null;
	}

	@Override
	public E peek() {
		return null;
	}
}