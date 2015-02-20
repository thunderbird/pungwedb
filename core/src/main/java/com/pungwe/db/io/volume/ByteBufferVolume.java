package com.pungwe.db.io.volume;

import java.io.*;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

// Inspired by MapDB (pretty much copied for now)...

/**
 * Created by 917903 on 04/02/2015.
 */
public abstract class ByteBufferVolume implements Volume {

	protected static final int VOLUME_PAGE_SHIFT = 20; // 1 MB

	protected final ReentrantLock growLock = new ReentrantLock(false);

	protected final int sliceShift;
	protected final int sliceSizeModMask;
	protected final int sliceSize;
	protected volatile long mark;
	protected AtomicLong position = new AtomicLong();

	protected volatile ByteBuffer[] slices = new ByteBuffer[0];

	protected final boolean readOnly;

	public ByteBufferVolume(boolean readOnly, int sliceShift) {
		this.readOnly = readOnly;
		this.sliceShift = sliceShift;
		this.sliceSize = 1 << sliceShift; // MAX_SIZE is 2GB
		this.sliceSizeModMask = sliceSize - 1;
	}

	public abstract ByteBuffer makeNewBuffer(long offset) throws IOException;

	@Override
	public void clear(long startOffset, long endOffset) throws IOException {
		ByteBuffer buf = slices[(int) (startOffset >>> sliceShift)];
		int start = (int) (startOffset & sliceSizeModMask);
		int end = (int) (endOffset & sliceSizeModMask);

		int pos = start;
		while (pos < end) {
			buf = buf.duplicate();
			buf.position(pos);
			buf.put(CLEAR, 0, Math.min(CLEAR.length, end - pos));
			pos += CLEAR.length;
		}
	}

	@Override
	public void mark() throws IOException {
		mark = getPosition();
	}

	@Override
	public long getPosition() throws IOException {
		return position.get();
	}

	@Override
	public void seek(long position) throws IOException {
		this.position.set(position);
	}

	// Copied from MapDB
	@Override
	public void ensureAvailable(long offset) throws IOException {
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

			for (int pos = oldSize; pos < slices2.length; pos++) {
				slices2[pos] = makeNewBuffer(1L * sliceSize * pos);
			}

			slices = slices2;
		} finally {
			growLock.unlock();
		}
	}

	@Override
	public String readUTF() throws IOException {
		return DataInputStream.readUTF(this);
	}

	@Override
	public int skipBytes(int n) throws IOException {
		long newPos = getPosition() + n;
		long oldPos = getPosition();
		long len = 0;
		if (n <= 0) {
			return 0;
		}
		newPos = oldPos + n;
		if (newPos > len) {
			newPos = len;
		}
		seek(newPos);

        /* return the actual number of bytes skipped */
		return (int) (newPos - oldPos);
	}

	@Override
	public boolean readBoolean() throws IOException {
		return slices[(int) (getPosition() >>> sliceShift)].get((int) (position.getAndIncrement() & sliceSizeModMask)) == 1;
	}

	@Override
	public byte readByte() throws IOException {
		return slices[(int) (getPosition() >>> sliceShift)].get((int) (position.getAndIncrement() & sliceSizeModMask));
	}

	@Override
	public int readUnsignedByte() throws IOException {
		int temp = this.readByte();
		if (temp < 0) {
			throw new EOFException();
		}
		return temp;
	}

	@Override
	public short readShort() throws IOException {
		return slices[(int) (getPosition() >>> sliceShift)].getShort((int) (position.getAndAdd(2) & sliceSizeModMask));
	}

	@Override
	public int readUnsignedShort() throws IOException {
		int ch1 = slices[(int) (getPosition() >>> sliceShift)].get((int) (position.getAndIncrement() & sliceSizeModMask)) & 0xFF;
		int ch2 = slices[(int) (getPosition() >>> sliceShift)].get((int) (position.getAndIncrement() & sliceSizeModMask)) & 0xFF;
		if ((ch1 | ch2) < 0)
			throw new EOFException();
		return (ch1 << 8) + (ch2 << 0);
	}

	@Override
	public char readChar() throws IOException {
		return slices[(int) (getPosition() >>> sliceShift)].getChar((int) (position.getAndIncrement() & sliceSizeModMask));
	}

	@Override
	public int readInt() throws IOException {
		return slices[(int) (getPosition() >>> sliceShift)].getInt((int) (position.getAndAdd(4) & sliceSizeModMask));
	}

	@Override
	public long readLong() throws IOException {
		return slices[(int) (getPosition() >>> sliceShift)].getLong((int) (position.getAndAdd(8) & sliceSizeModMask));
	}

	@Override
	public float readFloat() throws IOException {
		return slices[(int) (getPosition() >>> sliceShift)].getFloat((int) (position.getAndAdd(4) & sliceSizeModMask));
	}

	@Override
	public double readDouble() throws IOException {
		return slices[(int) (getPosition() >>> sliceShift)].getDouble((int) (position.getAndAdd(8) & sliceSizeModMask));
	}

	@Override
	public String readLine() throws IOException {
		StringBuffer input = new StringBuffer();
		int c = -1;
		boolean eol = false;

		while (!eol) {
			switch (c = readByte()) {
				case -1:
				case '\n':
					eol = true;
					break;
				case '\r':
					eol = true;
					long cur = getPosition();
					if ((readByte()) != '\n') {
						seek(cur);
					}
					break;
				default:
					input.append((char) c);
					break;
			}
		}

		if ((c == -1) && (input.length() == 0)) {
			return null;
		}
		return input.toString();
	}

	@Override
	public void readFully(byte[] b, int off, int len) throws IOException {
		int n = 0;
		ByteBuffer buffer = this.slices[(int) (getPosition() >>> sliceShift)].duplicate();
		buffer.position((int) (getPosition() & sliceSizeModMask));
		buffer.get(b, off, len);
		position.getAndAdd(len);
	}

	@Override
	public void readFully(byte[] b) throws IOException {
		readFully(b, 0, b.length);
	}

	@Override
	public void write(int b) throws IOException {
		slices[(int) (getPosition() >>> sliceShift)].put((int) (position.getAndIncrement() & sliceSizeModMask), (byte) b);
	}

	@Override
	public void write(byte[] b) throws IOException {
		write(b, 0, b.length);
	}

	@Override
	public void write(byte[] b, int off, int len) throws IOException {
		final ByteBuffer b1 = slices[(int) (getPosition() >>> sliceShift)].duplicate();
		final int bufPos = (int) (getPosition() & sliceSizeModMask);

		b1.position(bufPos);
		b1.put(b, off, len);
		position.addAndGet(len);
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
		slices[(int) (getPosition() >>> sliceShift)].putShort((int) (position.getAndAdd(2) & sliceSizeModMask), (short) v);
	}

	@Override
	public void writeChar(int v) throws IOException {
		slices[(int) (getPosition() >>> sliceShift)].putChar((int) (position.getAndIncrement() & sliceSizeModMask), (char) v);
	}

	@Override
	public void writeInt(int v) throws IOException {
		slices[(int) (getPosition() >>> sliceShift)].putInt((int) (position.getAndAdd(4) & sliceSizeModMask), v);
	}

	@Override
	public void writeLong(long v) throws IOException {
		slices[(int) (getPosition() >>> sliceShift)].putLong((int) (position.getAndAdd(8) & sliceSizeModMask), v);
	}

	@Override
	public void writeFloat(float v) throws IOException {
		slices[(int) (getPosition() >>> sliceShift)].putFloat((int) (position.getAndAdd(4) & sliceSizeModMask), v);
	}

	@Override
	public void writeDouble(double v) throws IOException {
		slices[(int) (getPosition() >>> sliceShift)].putDouble((int) (position.getAndAdd(8) & sliceSizeModMask), v);
	}

	@Override
	public void writeBytes(String s) throws IOException {
		byte bytes[] = new byte[s.length()];
		for (int index = 0; index < s.length(); index++) {
			bytes[index] = (byte) (s.charAt(index) & 0xFF);
		}
		write(bytes);
	}

	@Override
	public void writeChars(String s) throws IOException {
		byte newBytes[] = new byte[s.length() * 2];
		for (int index = 0; index < s.length(); index++) {
			int newIndex = index == 0 ? index : index * 2;
			newBytes[newIndex] = (byte) ((s.charAt(index) >> 8) & 0xFF);
			newBytes[newIndex + 1] = (byte) (s.charAt(index) & 0xFF);
		}
		write(newBytes);
	}

	@Override
	public void writeUTF(String s) throws IOException {
		int utfCount = 0, length = s.length();
		for (int i = 0; i < length; i++) {
			int charValue = s.charAt(i);
			if (charValue > 0 && charValue <= 127) {
				utfCount++;
			} else if (charValue <= 2047) {
				utfCount += 2;
			} else {
				utfCount += 3;
			}
		}
		if (utfCount > 65535) {
			throw new UTFDataFormatException(); //$NON-NLS-1$
		}
		byte utfBytes[] = new byte[utfCount + 2];
		int utfIndex = 2;
		for (int i = 0; i < length; i++) {
			int charValue = s.charAt(i);
			if (charValue > 0 && charValue <= 127) {
				utfBytes[utfIndex++] = (byte) charValue;
			} else if (charValue <= 2047) {
				utfBytes[utfIndex++] = (byte) (0xc0 | (0x1f & (charValue >> 6)));
				utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & charValue));
			} else {
				utfBytes[utfIndex++] = (byte) (0xe0 | (0x0f & (charValue >> 12)));
				utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & (charValue >> 6)));
				utfBytes[utfIndex++] = (byte) (0x80 | (0x3f & charValue));
			}
		}
		utfBytes[0] = (byte) (utfCount >> 8);
		utfBytes[1] = (byte) utfCount;
		write(utfBytes);
	}

	@Override
	public long getLength() throws IOException {
		return ((long)slices.length)*sliceSize;
	}

	@Override
	public DataInput getInput() {
		return this;
	}

	@Override
	public DataOutput getOutput() {
		return this;
	}


	/**
	 * Hack to unmap MappedByteBuffer.
	 * Unmap is necessary on Windows, otherwise file is locked until JVM exits or BB is GCed.
	 * There is no public JVM API to unmap buffer, so this tries to use SUN proprietary API for unmap.
	 * Any error is silently ignored (for example SUN API does not exist on Android).
	 */
	protected void unmap(MappedByteBuffer b) {
		try {
			if (unmapHackSupported) {

				// need to dispose old direct buffer, see bug
				// http://bugs.sun.com/bugdatabase/view_bug.do?bug_id=4724038
				Method cleanerMethod = b.getClass().getMethod("cleaner", new Class[0]);
				if (cleanerMethod != null) {
					cleanerMethod.setAccessible(true);
					Object cleaner = cleanerMethod.invoke(b);
					if (cleaner != null) {
						Method clearMethod = cleaner.getClass().getMethod("clean", new Class[0]);
						if (clearMethod != null)
							clearMethod.invoke(cleaner);
					}
				}
			}
		} catch (Exception e) {
			unmapHackSupported = false;
			//TODO exception handling
			//Utils.LOG.log(Level.WARNING, "ByteBufferVol Unmap failed", e);
		}
	}

	private static boolean unmapHackSupported = true;

	static {
		try {
			final ClassLoader loader = Thread.currentThread().getContextClassLoader();
			unmapHackSupported = Class.forName("sun.nio.ch.DirectBuffer", true, loader) != null;
		} catch (Exception e) {
			unmapHackSupported = false;
		}
	}

	// Workaround for https://github.com/jankotek/MapDB/issues/326
	// File locking after .close() on Windows.
	private static boolean windowsWorkaround = System.getProperty("os.name").toLowerCase().startsWith("win");

}
