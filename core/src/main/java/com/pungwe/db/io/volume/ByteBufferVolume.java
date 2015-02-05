package com.pungwe.db.io.volume;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.locks.ReentrantLock;

// Inspired by MapDB (pretty much copied for now)...
/**
 *
 * Created by 917903 on 04/02/2015.
 */
public abstract class ByteBufferVolume implements Volume {

	protected final ReentrantLock growLock = new ReentrantLock(false);

	private final int sliceShift;
	private final int sliceSizeModMask;
	private final int sliceSize;

	private volatile ByteBuffer[] slices = new ByteBuffer[0];

	protected final boolean readOnly;

	public ByteBufferVolume(boolean readOnly, int sliceShift) {
		this.readOnly = readOnly;
		this.sliceShift = sliceShift;
		this.sliceSize = 1 << sliceShift;
		this.sliceSizeModMask = sliceSize - 1;
	}

	public abstract ByteBuffer makeNewBuffer(long offset);

	@Override
	public void seek(long position) throws IOException {

	}

	@Override
	public long getPosition() {
		return 0;
	}

	@Override
	public long getLength() throws IOException {
		return 0;
	}

	@Override
	public long getPositionLimit() {
		return 0;
	}

	@Override
	public void mark() {

	}

	// Copied from MapDB
	@Override
	public void ensureAvailable(long offset) {
		int slicePos = (int) (offset >>> sliceShift);

		// check for most common case, this is already mapped
		if (slicePos < slices.length) {
			return;
		}

		growLock.lock();

		try {
			// Check a second time
			if (slicePos < slices.length) {
				return;
			}

			int oldSize = slices.length;
			ByteBuffer[] slices2 = slices;

			slices2 = Arrays.copyOf(slices2, Math.max(slicePos + 1, slices2.length + slices2.length / 1000));

			for(int pos=oldSize;pos<slices2.length;pos++) {
				slices2[pos]=makeNewBuffer(1L* sliceSize *pos);
			}

			slices = slices2;
		} finally {
			growLock.unlock();
		}
	}

	@Override
	public boolean isClosed() {
		return false;
	}

	@Override
	public String readUTF() throws IOException {
		return null;
	}

	@Override
	public int skipBytes(int n) throws IOException {
		return 0;
	}

	@Override
	public boolean readBoolean() throws IOException {
		return false;
	}

	@Override
	public byte readByte() throws IOException {
		return 0;
	}

	@Override
	public int readUnsignedByte() throws IOException {
		return 0;
	}

	@Override
	public short readShort() throws IOException {
		return 0;
	}

	@Override
	public int readUnsignedShort() throws IOException {
		return 0;
	}

	@Override
	public char readChar() throws IOException {
		return 0;
	}

	@Override
	public int readInt() throws IOException {
		return 0;
	}

	@Override
	public long readLong() throws IOException {
		return 0;
	}

	@Override
	public float readFloat() throws IOException {
		return 0;
	}

	@Override
	public double readDouble() throws IOException {
		return 0;
	}

	@Override
	public String readLine() throws IOException {
		return null;
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {

	}

	@Override
	public void readFully(byte[] b) throws IOException {

	}

	@Override
	public void write(int b) throws IOException {
		slices[(int)(getPosition() >>> sliceShift)].put((int)(getPosition() & sliceSizeModMask), (byte)b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		final ByteBuffer b1 = slices[(int)(getPosition() >>> sliceShift)].duplicate();
		final int bufPos = (int) (getPosition()& sliceSizeModMask);

		b1.position(bufPos);
		b1.put(b, off, len);
	}

	@Override
	public void writeBoolean(boolean v) throws IOException {
		write(v ? 1 : 0);
	}

	@Override
	public void writeByte(int v) throws IOException {
		write(v);
	}

	@Override
	public void writeShort(int v) throws IOException {
		slices[(int)(getPosition() >>> sliceShift)].putShort((int)(getPosition() & sliceSizeModMask), (short)v);
	}

	@Override
	public void writeChar(int v) throws IOException {
		slices[(int)(getPosition() >>> sliceShift)].putChar((int) (getPosition() & sliceSizeModMask), (char) v);
	}

	@Override
	public void writeInt(int v) throws IOException {
		slices[(int)(getPosition() >>> sliceShift)].putInt((int) (getPosition() & sliceSizeModMask), v);
	}

	@Override
	public void writeLong(long v) throws IOException {
		slices[(int)(getPosition() >>> sliceShift)].putLong((int)(getPosition() & sliceSizeModMask), v);
	}

	@Override
	public void writeFloat(float v) throws IOException {

	}

	@Override
	public void writeDouble(double v) throws IOException {

	}

	@Override
	public void writeBytes(String s) throws IOException {

	}

	@Override
	public void writeChars(String s) throws IOException {

	}

	@Override
	public void writeUTF(String s) throws IOException {

	}

	@Override
	public DataInput getInput() {
		return this;
	}

	@Override
	public DataOutput getOutput() {
		return this;
	}


}
