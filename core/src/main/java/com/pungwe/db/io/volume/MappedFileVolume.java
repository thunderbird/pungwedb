package com.pungwe.db.io.volume;

import com.pungwe.db.constants.TypeReference;

import java.io.*;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 * Created by 917903 on 04/02/2015.
 */
public class MappedFileVolume implements Volume {

	private static final int TWOGIG = Integer.MAX_VALUE;

	private MappedByteBuffer[] segments;
	private RandomAccessFile file;
	private long length;
	private long increment;
	private final long maxLength;
	private long position;
	private long mark;
	private volatile boolean closed = false;


	public MappedFileVolume(File file, long increment, long maxFileSize) throws IOException {
		this.file = new RandomAccessFile(file, "rw");
		this.length = file.length() == 0 ? increment : file.length();
		this.increment = increment;
		maxLength = maxFileSize;

		if (increment <= 0) {
			throw new IllegalArgumentException("Increment must be above 0");
		}

		if (this.file.length() == 0) {
			this.file.setLength(this.increment);
		}

	}

	private void map(long newLength) throws IOException {
		long segCount = (long) Math.ceil((double) newLength / TWOGIG);
		if (segCount > TWOGIG) {
			throw new ArithmeticException("Requested File Size is too large");
		}
		segments = new MappedByteBuffer[(int)segCount];
		long countDown = newLength;
		long from = 0;
		int seg = 0;
		FileChannel channel = this.file.getChannel();
		while (countDown > 0) {
			long len = Math.min(TWOGIG, countDown);
			MappedByteBuffer buffer = channel.map(FileChannel.MapMode.READ_WRITE, from, len);
			segments[seg++] = buffer;
			from += len;
			countDown -= len;
		}
	}

	private void expand() throws IOException {
		long newLength = this.file.length() + increment;
		if (maxLength > 0 && newLength > maxLength) {
			throw new IOException("Cannot expand beyond max size of: " + ((double)maxLength / 1024 / 1024) + "GB");
		}
		this.file.setLength(newLength);
		map(newLength);
	}

	private int determineSegment(long offset, int size) {
		double a = offset;
		double b = TWOGIG;

		int whichChunk = (int)Math.floor(a / b);

		return whichChunk;
	}

	private int determinePosition(long offset, int chunk, int size) {
		return (int)offset - chunk * TWOGIG;
	}

	// This needs to be re-engineered
	/*private void write(long offSet, byte[] src, int off, int len) throws IOException {
		// Quick and dirty but will go wrong for massive numbers ??
		double a = offSet;
		double b = TWOGIG;
		long whichChunk = (long) Math.floor(a / b);
		long withinChunk = offSet - whichChunk * TWOGIG;

		try {
			if (src.length + 1 + offSet > this.file.length()) {
				expand();
			}
			ByteBuffer segment = segments[(int)whichChunk];
			long remaining = segment.capacity() - withinChunk;
			if (remaining > src.length + 1) {
				// Allows free threading
				ByteBuffer writeBuffer = segment.duplicate();
				writeBuffer.position((int) withinChunk);
				writeBuffer.put(src, 0, src.length);
			} else {
				int l1 = (int) (segment.capacity() - (withinChunk + 1));
				int l2 = (int) (src.length - 1) - l1;

				// Allows free threading
				ByteBuffer writeBuffer = segment.duplicate();
				writeBuffer.position((int) withinChunk);
				writeBuffer.put(src, 0, l1);

				segment = segments[(int) whichChunk + 1];
				writeBuffer = segment.duplicate();
				writeBuffer.position(0);
				writeBuffer.put(src, l1, l2);

			}
		} catch (BufferOverflowException ex) {
			throw ex;
		}
	} */

	@Override
	public void seek(long position) throws IOException {
		// do nothing and just set the position in the file
		synchronized (file) {
			if (position < this.file.length()) {
				this.position = position;
			}
		}
	}

	@Override
	public long getPosition() {
		return position;
	}

	@Override
	public long getLength() throws IOException {
		synchronized (file) {
			return file.length();
		}
	}

	@Override
	public long getPositionLimit() {
		return 0;
	}

	@Override
	public void mark() {
		mark = position;
	}

	@Override
	public void ensureAvailable(long offset) {
		//
	}

	@Override
	public boolean isClosed() {
		return false;
	}

	@Override
	public DataInput getInput() {
		return this;
	}

	@Override
	public DataOutput getOutput() {
		return this;
	}

	@Override
	public void close() throws IOException {
		synchronized (file) {
			file.close();
			closed = true;
		}
	}
}
