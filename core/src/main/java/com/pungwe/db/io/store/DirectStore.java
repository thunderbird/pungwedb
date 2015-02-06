package com.pungwe.db.io.store;


import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.volume.Volume;
import com.pungwe.db.types.Header;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by 917903 on 05/02/2015.
 */
public class DirectStore implements Store {

	// FIXME: Centralise this....
	//private static final int PAGE_SIZE = 1 << 20;
	private static final int PAGE_SIZE = 4096;
	protected final Volume volume;
	protected final DirectStoreHeader header;

	/** protects structural layout of records. Memory allocator is single threaded under this lock */
	protected final ReentrantLock structuralLock = new ReentrantLock(false);

	/** protects lifecycle methods such as commit, rollback and close() */
	protected final ReentrantLock commitLock = new ReentrantLock(false);

	public DirectStore(Volume volume) throws IOException {
		this.volume = volume;

		if (volume.getLength() == 0) {
			this.header = new DirectStoreHeader(PAGE_SIZE, PAGE_SIZE);
			synchronized (volume) {
				volume.ensureAvailable(PAGE_SIZE);
				volume.clear(0, PAGE_SIZE);
			}
			writeHeader();
		} else {
			header = findHeader();
		}
	}

	private void writeHeader() throws IOException {

		// Get the first segment and write the header
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bytes);
		new DirectStoreHeaderSerializer().serialize(out, this.header);
		// We must not exceed PAGE_SIZE
		byte[] data = bytes.toByteArray();
		assert data.length < PAGE_SIZE - 5 : "Header is larger than a block...";

		volume.seek(0);
		volume.writeByte(TypeReference.HEADER.getType());
		volume.writeInt(data.length);
		volume.write(data);
	}

	private DirectStoreHeader findHeader() throws IOException {
		long current = 0;
		while (current < volume.getLength()) {
			byte[] buffer = new byte[PAGE_SIZE];
			this.volume.readFully(buffer);
			byte firstByte = buffer[0];
			if (firstByte == TypeReference.HEADER.getType()) {
				ByteArrayInputStream bytes = new ByteArrayInputStream(buffer);
				DataInputStream in = new DataInputStream(bytes);
				in.skip(5);
				return new DirectStoreHeaderSerializer().deserialize(in);
			}
			current += PAGE_SIZE;
		}
		throw new IOException("Could not find file header. File could be wrong or corrupt");
	}

	@Override
	public <T> long put(T value, Serializer<T> serializer) throws IOException {
		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bytes);
		serializer.serialize(out, value);
		byte[] data = bytes.toByteArray();
		int pages = (int) Math.ceil((double) data.length / header.getBlockSize());
		long position = getHeader().getNextPosition(pages * header.getBlockSize());
		synchronized (volume) {
			this.volume.ensureAvailable(position);
			this.volume.seek(position);
			this.volume.writeByte(TypeReference.OBJECT.getType());
			this.volume.writeInt(data.length);
			this.volume.write(data);
		}
		// Update the header
		synchronized (header) {
			writeHeader();
		}

		return position;
	}

	@Override
	public <T> T get(long position, Serializer<T> serializer) throws IOException {
		byte[] data = new byte[0];
		synchronized (volume) {
			// We need to read the first few bytes
			this.volume.seek(position);
			TypeReference t = TypeReference.fromType(this.volume.readByte());
			assert t != null : "Cannot determine type";

			data = new byte[this.volume.readInt()];
			this.volume.readFully(data);
		}

		if (data.length == 0) {
			return null;
		}

		ByteArrayInputStream is = new ByteArrayInputStream(data);
		DataInputStream in = new DataInputStream(is);
		T value = serializer.deserialize(in);

		return value;
	}

	@Override
	public <T> long update(long position, T value, Serializer<T> serializer) throws IOException {

		this.volume.ensureAvailable(position);

		// First things first, we need to find the appropriate record and find out how big it is
		int size = 0;
		synchronized (volume) {
			volume.seek(position);
			TypeReference type = TypeReference.fromType(volume.readByte());
			size = volume.readInt();
		}

		int origPageSize = (int) Math.ceil(((double)size + 5) / header.getBlockSize());

		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bytes);
		serializer.serialize(out, value);
		byte[] data = bytes.toByteArray();
		int pages = (int) Math.ceil((double) data.length / header.getBlockSize());

		if (pages > origPageSize) {
			position = getHeader().getNextPosition(pages * header.getBlockSize());
		}

		synchronized (volume) {
			volume.seek(position);
			volume.writeByte(TypeReference.OBJECT.getType());
			volume.writeInt(data.length);
			volume.write(data);
		}

		synchronized (header) {
			writeHeader();
		}
		return position;
	}

	@Override
	public Header getHeader() {
		return header;
	}

	@Override
	public void remove(long position) throws IOException {
		synchronized (volume) {
			this.volume.ensureAvailable(position);
			volume.seek(position);
			assert TypeReference.fromType(volume.readByte()) != null : "Cannot determine type";
			volume.seek(position);
			volume.writeByte(TypeReference.DELETED.getType());
		}

		synchronized (header) {
			writeHeader();
		}
	}

	@Override
	public void commit() throws IOException {

	}

	@Override
	public void rollback() throws UnsupportedOperationException, IOException {

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
		return false;
	}

	@Override
	public long alloc(long size) throws IOException {
		synchronized (header) {
			int pages = (int) Math.ceil((double) size / header.getBlockSize());
			return header.getNextPosition(pages * header.getBlockSize());
		}
	}

	@Override
	public void close() throws IOException {
		volume.close();
	}

	private static class DirectStoreHeader extends Header {

		public DirectStoreHeader(int blockSize) {
			super(blockSize, DirectStoreHeader.class.getName());
		}

		public DirectStoreHeader(int blockSize, long currentPosition) {
			super(blockSize, currentPosition, DirectStoreHeader.class.getName());
		}
	}

	private class DirectStoreHeaderSerializer implements Serializer<DirectStoreHeader> {

		@Override
		public void serialize(DataOutput out, DirectStoreHeader value) throws IOException {
			out.writeUTF(value.getStore());
			out.writeInt(value.getBlockSize());
			out.writeLong(value.getPosition());
			out.writeLong(value.getMetaData());
		}

		@Override
		public DirectStoreHeader deserialize(DataInput in) throws IOException {
			String store = in.readUTF();
			assert store.equals(DirectStoreHeader.class.getName());
			int blockSize = in.readInt();
			long nextPosition = in.readLong();
			long metaData = in.readLong();
			DirectStoreHeader header = new DirectStoreHeader(blockSize, nextPosition);
			header.setMetaData(metaData);
			return header;
		}

		@Override
		public TypeReference getTypeReference() {
			return TypeReference.HEADER;
		}

	}
}
