package com.pungwe.db.io.volume;

import java.io.*;

/**
 * Created by 917903 on 04/02/2015.
 */
public class RandomAccessFileVolume implements Volume {

	protected final RandomAccessFile file;
	protected long mark, position;
	protected volatile boolean closed = false;

	public RandomAccessFileVolume(File file, boolean readOnly) throws IOException {
		this.file = new RandomAccessFile(file, readOnly ? "r" : "rw");
	}

	@Override
	public void seek(long position) throws IOException {
		this.file.seek(position);
		this.position = position;
	}

	@Override
	public long getPosition() {
		return position;
	}

	@Override
	public long getLength() throws IOException {
		return this.file.length();
	}

	@Override
	public long getPositionLimit() {
		return -1;
	}

	@Override
	public void mark() {
		mark = getPosition();
	}

	@Override
	public DataInput getInput() {
		return this.file;
	}

	@Override
	public DataOutput getOutput() {
		return this.file;
	}

	@Override
	public void close() throws IOException {
		this.file.close();
		closed = true;
	}

	@Override
	public void ensureAvailable(long offset) {
		// Do nothing
	}

	@Override
	public boolean isClosed() {
		return closed;
	}
}
