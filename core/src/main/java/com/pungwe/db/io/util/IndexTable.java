package com.pungwe.db.io.util;

import com.pungwe.db.io.volume.Volume;

import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by 917903 on 09/03/2015.
 *
 * Indexes records by record id.
 *
 * The offset is the record id and the value is the records offset in it's data file. This is used for fine grained
 * locking on collections...
 *
 *
 */
public final class IndexTable implements Iterable<Long> {

	final Volume volume;
	final ReentrantLock structuralLock = new ReentrantLock(false);
	final AtomicLong recordId = new AtomicLong(0);

	public IndexTable(Volume volume) throws IOException {
		this.volume = volume;
		if (this.volume.getLength() > 0) {
			recordId.set(this.volume.getInput(0).readLong());
		}
	}

	/**
	 *
	 * @param id
	 * @param offset
	 * @throws IOException
	 */
	public void updateRecord(long id, long offset) throws IOException {
		volume.getOutput(id).writeLong(offset);
	}

	/**
	 *
	 * @param offset
	 * @return
	 * @throws IOException
	 */
	public long addRecord(long offset) throws IOException {
		long position = recordId.addAndGet(8);
		volume.ensureAvailable(position);
		volume.getOutput(position).writeLong(offset);
		volume.getOutput(0).writeLong(recordId.get());
		return position;
	}

	/**
	 *
	 * @param id
	 * @return
	 * @throws IOException
	 */
	public long getOffset(long id) throws IOException {
		return volume.getInput(id).readLong();
	}

	@Override
	public Iterator<Long> iterator() {
		return new OffsetIterator();
	}

	public long getCurrent() {
		return recordId.get();
	}

	public long getFirst() {
		return 8;
	}

	final class OffsetIterator implements Iterator<Long> {

		protected AtomicLong current = new AtomicLong(8); // first record always starts at 8

		@Override
		public boolean hasNext() {
			try {
				final long lastRecord = volume.getInput(0).readLong();
				return current.get() <= lastRecord;
			} catch (Throwable t) {
				// Log message here
			}
			return false;
		}

		@Override
		public Long next() {
			return current.getAndAdd(8l); // get the current record and increment by 8
		}
	}
}
