package com.pungwe.db.io.store;

import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.io.FastByteArrayOutputStream;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.volume.Volume;
import com.pungwe.db.types.Header;
import java.io.*;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Created by 917903 on 04/02/2015.
 */
public class AppendOnlyStore implements Store {

	public static final int PAGE_SIZE = 4096;

	protected final Volume volume;
	protected volatile AppendOnlyHeader header;

	/**
	 * protects lifecycle methods such as commit, rollback and close()
	 */
	protected final ReentrantLock commitLock = new ReentrantLock(false);

	public AppendOnlyStore(Volume volume) throws IOException {
		this.volume = volume;
		if (volume.getLength() == 0) {
			header = new AppendOnlyHeader(PAGE_SIZE);
			writeHeader();
		} else {
			header = findHeader();
		}
	}

	@Override
	public long alloc(long size) throws IOException {
		try {
			commitLock.lock();
			// Depending on the volume, this may not do anything
			volume.ensureAvailable(header.getPosition() + size);
			return header.getNextPosition(size);
		} finally {
			commitLock.unlock();
		}
	}

	@Override
	public <T> long put(T value, Serializer<T> serializer) throws IOException {
		try {
			commitLock.lock();

			FastByteArrayOutputStream out = new FastByteArrayOutputStream();
			serializer.serialize(new DataOutputStream(out), value);
			// Get the data as an array
			byte[] data = out.toByteArray();
			// Get the length of data
			double length = data.length + 5;
			// Calculate the number of pages
			int pages = (int) Math.ceil(length / header.getBlockSize());

			// Always take the position from the last header...
			long position = alloc(pages * header.getBlockSize());
			DataOutput output = this.volume.getOutput(position);
			output.write(TypeReference.OBJECT.getType());
			output.writeInt(data.length);
			output.write(data);
			return position;
		} finally {
			commitLock.unlock();
		}
	}

	@Override
	public <T> T get(long position, Serializer<T> serializer) throws IOException {
		DataInput input = volume.getInput(position);
		byte b = input.readByte();
		assert TypeReference.fromType(b) != null : "Cannot determine type: " + b;
		int len = input.readInt();
		T value = serializer.deserialize(input);
		return value;
	}

	@Override
	public <T> long update(long position, T value, Serializer<T> serializer) throws IOException {
		return put(value, serializer);
	}

	@Override
	public Header getHeader() {
		return header;
	}

	@Override
	public void remove(long position) throws IOException {

	}

	@Override
	public void commit() throws IOException {
		try {
			commitLock.lock();
			// Update the header
			synchronized (header) {
				writeHeader();
			}
		} finally {
			commitLock.unlock();
		}
	}

	@Override
	public void rollback() throws UnsupportedOperationException, IOException {
		try {
			commitLock.lock();
			synchronized (header) {
				this.header = findHeader(volume.getLength() - (PAGE_SIZE * 2));
				writeHeader(); // rewrite the header just in case
			}
		} finally {
			commitLock.unlock();
		}
	}

	@Override
	public boolean isClosed() throws IOException {
		return volume.isClosed();
	}

	@Override
	public void lock(long position, int size) {
	}

	@Override
	public void unlock(long position, int size) {
	}

	@Override
	public boolean isAppendOnly() {
		return true;
	}

	@Override
	public void close() throws IOException {
		try {
			commitLock.lock();
			volume.close();
		} finally {
			commitLock.unlock();
		}
	}

	protected AppendOnlyHeader findHeader() throws IOException {
		return findHeader(volume.getLength() - PAGE_SIZE);
	}

	protected AppendOnlyHeader findHeader(long position) throws IOException {
		long current = position;
		while (current > 0) {
			byte[] buffer = new byte[PAGE_SIZE];
			DataInput input = this.volume.getInput(current);
			input.readFully(buffer);
			byte firstByte = buffer[0];
			if (firstByte == TypeReference.HEADER.getType()) {
				ByteArrayInputStream bytes = new ByteArrayInputStream(buffer);
				DataInputStream in = new DataInputStream(bytes);
				in.skip(5);
				return new AppendOnlyFileSerializer().deserialize(in);
			}
			current -= PAGE_SIZE;
		}
		throw new IOException("Could not find file header. File could be wrong or corrupt");
	}

	protected void writeHeader() throws IOException {
		synchronized (header) {
			// allocate PAGE_SIZE to the header, so it's up to date when writing
			long position = alloc(PAGE_SIZE);
			// Get the first segment and write the header
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(bytes);
			new AppendOnlyFileSerializer().serialize(out, this.header);
			// We must not exceed PAGE_SIZE
			byte[] data = bytes.toByteArray();
			assert data.length < PAGE_SIZE - 5 : "Header is larger than a block...";
			DataOutput output = volume.getOutput(position);
			output.write(TypeReference.HEADER.getType());
			output.writeInt(data.length);
			output.write(data);
			if (data.length < (PAGE_SIZE - 5)) {
				output.write(new byte[(PAGE_SIZE - 5 - data.length)]);
			}
		}
	}

	private static class AppendOnlyHeader extends Header {

		public AppendOnlyHeader(int blockSize) {
			super(blockSize, AppendOnlyStore.class.getName());
		}

		public AppendOnlyHeader(int blockSize, long currentPosition) {
			super(blockSize, currentPosition, AppendOnlyStore.class.getName());
		}
	}

	private class AppendOnlyFileSerializer implements Serializer<AppendOnlyHeader> {

		@Override
		public void serialize(DataOutput out, AppendOnlyHeader value) throws IOException {
			out.writeUTF(value.getStore());
			out.writeInt(value.getBlockSize());
			out.writeLong(value.getPosition());
			out.writeLong(value.getMetaData());
		}

		@Override
		public AppendOnlyHeader deserialize(DataInput in) throws IOException {
			String store = in.readUTF();
			assert store.equals(AppendOnlyStore.class.getName());
			int blockSize = in.readInt();
			long nextPosition = in.readLong();
			long metaData = in.readLong();
			AppendOnlyHeader header = new AppendOnlyHeader(blockSize, nextPosition);
			header.setMetaData(metaData);
			return header;
		}

		@Override
		public TypeReference getTypeReference() {
			return TypeReference.HEADER;
		}

	}
}
