package com.pungwe.db.io.store;

import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.volume.Volume;
import com.pungwe.db.types.Header;
import sun.misc.LRUCache;

import java.io.*;


/**
 * Created by 917903 on 04/02/2015.
 */
public class AppendOnlyStore implements Store {

	public static final int PAGE_SIZE = 4096;

	protected final Volume volume;
	protected volatile AppendOnlyHeader header;

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
		synchronized (volume) {
			// Depending on the volume, this may not do anything
			volume.ensureAvailable(volume.getLength() + 1);
			return header.getNextPosition(size);
		}
	}

	@Override
	public <T> long put(T value, Serializer<T> serializer) throws IOException {
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		serializer.serialize(new DataOutputStream(out), value);
		// Get the data as an array
		byte[] data = out.toByteArray();
		// Get the length of data
		double length = data.length;
		// Calculate the number of pages
		int pages = (int) Math.ceil(length / header.getBlockSize());

		synchronized (volume) {
			// Always take the position from the last header... If the file is corrupt, we only
			long position = alloc(pages * header.getBlockSize());
			// Write the data to the volume
			this.volume.seek(position);
			this.volume.write(TypeReference.OBJECT.getType());
			this.volume.writeInt(data.length);
			this.volume.write(data);
			return position;
		}

	}

	@Override
	public <T> T get(long position, Serializer<T> serializer) throws IOException {
		synchronized (volume) {
			this.volume.seek(position);
			byte b = this.volume.readByte();
			assert TypeReference.fromType(b) != null : "Cannot determine type: " + b;
			int len = this.volume.readInt();
			byte[] buf = new byte[len];
			this.volume.readFully(buf);
			ByteArrayInputStream is = new ByteArrayInputStream(buf);
			DataInputStream in = new DataInputStream(is);
			T value = serializer.deserialize(in);
			return value;
		}
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
		// Update the header
		synchronized (header) {
			writeHeader();
		}
	}

	@Override
	public void rollback() throws UnsupportedOperationException, IOException {
		synchronized (header) {
			this.header = findHeader(volume.getLength() - (PAGE_SIZE * 2));
			writeHeader(); // rewrite the header just in case
		}
	}

	@Override
	public boolean isClosed() throws IOException {
		return false;
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
	public void close() throws IOException {
		volume.close();
	}

	private AppendOnlyHeader findHeader() throws IOException {
		return findHeader(volume.getLength() - PAGE_SIZE);
	}

	private AppendOnlyHeader findHeader(long position) throws IOException {
		long current = position;
		while (current > 0) {
			byte[] buffer = new byte[PAGE_SIZE];
			this.volume.readFully(buffer);
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

	private void writeHeader() throws IOException {
		synchronized (header) {
			// Get the first segment and write the header
			ByteArrayOutputStream bytes = new ByteArrayOutputStream();
			DataOutputStream out = new DataOutputStream(bytes);
			new AppendOnlyFileSerializer().serialize(out, this.header);
			// We must not exceed PAGE_SIZE
			byte[] data = bytes.toByteArray();
			assert data.length < PAGE_SIZE - 5 : "Header is larger than a block...";
			synchronized (volume) {
				long position = alloc(data.length);
				volume.seek(position);
				volume.write(TypeReference.HEADER.getType());
				volume.writeInt(data.length);
				volume.write(data);
				if (data.length < (PAGE_SIZE - 5)) {
					this.volume.write(new byte[(PAGE_SIZE - 5) - data.length]);
				}
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
