package com.pungwe.db.io.store;


import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.volume.Volume;
import com.pungwe.db.types.Header;

import java.io.*;
import java.lang.reflect.Type;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Created by 917903 on 05/02/2015.
 */
public class DirectStore implements Store {

	// FIXME: Centralise this....
	//private static final int BLOCK_SIZE = 1 << 20;
	private static final int BLOCK_SIZE = 4096;
	protected final Volume volume;
	protected final DirectStoreHeader header;

	/** protects structural layout of records. Memory allocator is single threaded under this lock */
	protected final ReentrantLock structuralLock = new ReentrantLock(false);

	public DirectStore(Volume volume) throws IOException {
		this.volume = volume;

		if (volume.getLength() == 0) {
			this.header = new DirectStoreHeader(BLOCK_SIZE, BLOCK_SIZE);
			volume.ensureAvailable(BLOCK_SIZE);
			volume.clear(0, BLOCK_SIZE);
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
		// We must not exceed BLOCK_SIZE
		byte[] data = bytes.toByteArray();
		assert data.length < BLOCK_SIZE - 5 : "Header is larger than a block...";

		DataOutput output = volume.getOutput(0);
		output.write(TypeReference.HEADER.getType());
		output.write(data.length);
		output.write(data);
	}

	private DirectStoreHeader findHeader() throws IOException {
		long current = 0;
		while (current < volume.getLength()) {
			byte[] buffer = new byte[BLOCK_SIZE];
			DataInput input = volume.getInput(0);
			input.readFully(buffer);
			byte firstByte = buffer[0];
			if (firstByte == TypeReference.HEADER.getType()) {
				ByteArrayInputStream bytes = new ByteArrayInputStream(buffer);
				DataInputStream in = new DataInputStream(bytes);
				in.skip(5);
				return new DirectStoreHeaderSerializer().deserialize(in);
			}
			current += BLOCK_SIZE;
		}
		throw new IOException("Could not find file header. File could be wrong or corrupt");
	}

	@Override
	public <T> long put(T value, Serializer<T> serializer) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		serializer.serialize(new DataOutputStream(out), value);
		// Get the data as an array
		byte[] data = out.toByteArray();
		// Get the length of data
		double length = data.length + 5;
		// Calculate the number of pages
		int pages = (int) Math.ceil(length / header.getBlockSize());

		// Always take the position from the last header...
		long position = alloc(pages * header.getBlockSize());
		volume.ensureAvailable(position + (header.getBlockSize() * pages));
		DataOutput output = this.volume.getOutput(position);
		output.write(TypeReference.OBJECT.getType());
		output.writeInt(data.length);
		output.write(data);

		// Update the header
		synchronized (header) {
			writeHeader();
		}

		return position;
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

		this.volume.ensureAvailable(position);

		// First things first, we need to find the appropriate record and find out how big it is
		int size = 0;

		DataInput input = volume.getInput(position);
		TypeReference type = TypeReference.fromType(input.readByte());
		size = input.readInt();


		int origPageSize = (int) Math.ceil(((double)size + 5) / header.getBlockSize());

		ByteArrayOutputStream bytes = new ByteArrayOutputStream();
		DataOutputStream out = new DataOutputStream(bytes);
		serializer.serialize(out, value);
		byte[] data = bytes.toByteArray();
		int pages = (int) Math.ceil((double) (data.length + 5) / header.getBlockSize());

		if (pages > origPageSize) {
			position = getHeader().getNextPosition(pages * header.getBlockSize());
		}


		DataOutput output = volume.getOutput(position);
		output.writeByte(TypeReference.OBJECT.getType());
		output.writeInt(data.length);
		output.write(data);

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
		this.volume.ensureAvailable(position);
		DataInput input = volume.getInput(position);
		assert TypeReference.fromType(input.readByte()) != null : "Cannot determine type";
		DataOutput output = volume.getOutput(position);
		output.writeByte(TypeReference.DELETED.getType());

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
			super(blockSize, DirectStore.class.getName());
		}

		public DirectStoreHeader(int blockSize, long currentPosition) {
			super(blockSize, currentPosition, DirectStore.class.getName());
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
