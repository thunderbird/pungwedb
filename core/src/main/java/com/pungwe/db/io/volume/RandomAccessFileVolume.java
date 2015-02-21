package com.pungwe.db.io.volume;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 917903 on 04/02/2015.
 */
public class RandomAccessFileVolume implements Volume {

	protected final RandomAccessFile file;
	protected volatile boolean closed = false;

	public RandomAccessFileVolume(File file, boolean readOnly) throws IOException {
		this.file = new RandomAccessFile(file, readOnly ? "r" : "rw");
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
	public final DataInput getInput(long offset) {
		return new RandomAccessFileVolumeDataInput(file, offset);
	}

	@Override
	public final DataOutput getOutput(long offset) {
		return new RandomAccessFileVolumeDataOutput(file, offset);
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

	@Override
	public void clear(long startOffset, long endOffset) throws IOException {
		file.seek(startOffset);
		while (startOffset < endOffset) {
			long remaining = Math.min(CLEAR.length, endOffset - startOffset);
			file.write(CLEAR, 0, (int) remaining);
			startOffset += CLEAR.length;
		}
	}

	private static final class RandomAccessFileVolumeDataInput implements DataInput {

		private final FileChannel channel;
		private final AtomicLong offset;

		public RandomAccessFileVolumeDataInput(RandomAccessFile file, long offset) {
			channel = file.getChannel();
			this.offset = new AtomicLong(offset);
		}

		@Override
		public void readFully(byte[] b) throws IOException {
			readFully(b, 0, b.length);
		}

		@Override
		public void readFully(byte[] b, int off, int len) throws IOException {
			ByteBuffer buffer = ByteBuffer.wrap(b, off, len);
			channel.read(buffer, offset.getAndAdd(b.length));
		}

		@Override
		public int skipBytes(int n) throws IOException {
			offset.getAndAdd(n);
			return n;
		}

		@Override
		public boolean readBoolean() throws IOException {
			byte b = readByte();
			return b == 1;
		}

		@Override
		public byte readByte() throws IOException {
			ByteBuffer buffer = ByteBuffer.allocate(1);
			channel.read(buffer, offset.getAndIncrement());
			return buffer.get();
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
			ByteBuffer buffer = ByteBuffer.allocate(2);
			channel.read(buffer, offset.getAndAdd(2));
			return buffer.getShort();
		}

		@Override
		public int readUnsignedShort() throws IOException {

			int ch1 = readByte() & 0xFF;
			int ch2 = readByte() & 0xFF;

			if ((ch1 | ch2) < 0) {
				throw new EOFException();
			}

			return (ch1 << 8) + (ch2 << 0);
		}

		@Override
		public char readChar() throws IOException {
			ByteBuffer buffer = ByteBuffer.allocate(1);
			channel.read(buffer, offset.getAndIncrement());
			return buffer.getChar();
		}

		@Override
		public int readInt() throws IOException {
			ByteBuffer buffer = ByteBuffer.allocate(4);
			channel.read(buffer, offset.getAndAdd(4));
			return buffer.getInt();
		}

		@Override
		public long readLong() throws IOException {
			ByteBuffer buffer = ByteBuffer.allocate(8);
			channel.read(buffer, offset.getAndAdd(8));
			return buffer.getLong();
		}

		@Override
		public float readFloat() throws IOException {
			ByteBuffer buffer = ByteBuffer.allocate(4);
			channel.read(buffer, offset.getAndAdd(4));
			return buffer.getFloat();
		}

		@Override
		public double readDouble() throws IOException {
			ByteBuffer buffer = ByteBuffer.allocate(8);
			channel.read(buffer, offset.getAndAdd(8));
			return buffer.getDouble();
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
						long cur = offset.get();
						if ((readByte()) != '\n') {
							offset.set(cur);
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
		public String readUTF() throws IOException {
			return DataInputStream.readUTF(this);
		}
	}

	private static final class RandomAccessFileVolumeDataOutput implements DataOutput {

		private final FileChannel channel;
		private AtomicLong offset;

		public RandomAccessFileVolumeDataOutput(RandomAccessFile file, long offset) {
			channel = file.getChannel();
			this.offset = new AtomicLong(offset);
		}

		@Override
		public void write(int b) throws IOException {
			ByteBuffer buffer = ByteBuffer.allocate(1);
			buffer.put((byte)b);
			channel.write(buffer, offset.getAndIncrement());
		}

		@Override
		public void write(byte[] b) throws IOException {
			write(b, 0, b.length);
		}

		@Override
		public void write(byte[] b, int off, int len) throws IOException {
			ByteBuffer buffer = ByteBuffer.allocate(len);
			buffer.put(b, off, len);
			channel.write(buffer, offset.getAndAdd(len));
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
			ByteBuffer buffer = ByteBuffer.allocate(2);
			buffer.putShort((short)v);
			channel.write(buffer, offset.getAndAdd(2));
		}

		@Override
		public void writeChar(int v) throws IOException {
			ByteBuffer buffer = ByteBuffer.allocate(1);
			buffer.putChar((char)v);
			channel.write(buffer, offset.getAndIncrement());
		}

		@Override
		public void writeInt(int v) throws IOException {
			ByteBuffer buffer = ByteBuffer.allocate(4);
			buffer.putInt(v);
			channel.write(buffer, offset.getAndAdd(4));
		}

		@Override
		public void writeLong(long v) throws IOException {
			ByteBuffer buffer = ByteBuffer.allocate(8);
			buffer.putLong(v);
			channel.write(buffer, offset.getAndAdd(8));
		}

		@Override
		public void writeFloat(float v) throws IOException {
			ByteBuffer buffer = ByteBuffer.allocate(4);
			buffer.putFloat(v);
			channel.write(buffer, offset.getAndAdd(4));
		}

		@Override
		public void writeDouble(double v) throws IOException {
			ByteBuffer buffer = ByteBuffer.allocate(8);
			buffer.putDouble(v);
			channel.write(buffer, offset.getAndAdd(8));
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
	}
}
