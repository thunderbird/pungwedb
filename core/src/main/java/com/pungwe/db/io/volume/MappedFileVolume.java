package com.pungwe.db.io.volume;

import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.exception.DBException;

import java.io.*;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.ArrayList;

/**
 * Created by 917903 on 04/02/2015.
 */
public class MappedFileVolume extends ByteBufferVolume {

	protected final File file;
	protected final FileChannel fileChannel;
	protected final FileChannel.MapMode mapMode;
	protected final java.io.RandomAccessFile raf;

	public MappedFileVolume(File file, boolean readOnly) throws IOException {
		this(file, readOnly, VOLUME_PAGE_SHIFT);
	}

	public MappedFileVolume(File file, boolean readOnly, int sliceShift) throws IOException {
		super(readOnly, sliceShift);

		this.file = file;
		this.mapMode = readOnly ? FileChannel.MapMode.READ_ONLY : FileChannel.MapMode.READ_WRITE;
		checkFolder(file, readOnly);
		this.raf = new java.io.RandomAccessFile(file, readOnly ? "r" : "rw");
		this.fileChannel = raf.getChannel();

		final long fileSize = fileChannel.size();
		if (fileSize > 0) {
			//map existing data
			slices = new ByteBuffer[(int) ((fileSize >>> sliceShift))];
			for (int i = 0; i < slices.length; i++) {
				slices[i] = makeNewBuffer(1L * i * sliceSize);
			}
		} else {
			slices = new ByteBuffer[0];
		}
	}

	private static void checkFolder(File file, boolean readOnly) throws IOException {
		File parent = file.getParentFile();
		if (parent == null) {
			parent = file.getCanonicalFile().getParentFile();
		}
		if (parent == null) {
			throw new IOException("Parent folder could not be determined for: " + file);
		}
		if (!parent.exists() || !parent.isDirectory()) {
			throw new IOException("Parent folder does not exist: " + file);
		}
		if (!parent.canRead()) {
			throw new IOException("Parent folder is not readable: " + file);
		}
		if (!readOnly && !parent.canWrite()) {
			throw new IOException("Parent folder is not writable: " + file);
		}
	}

	@Override
	public ByteBuffer makeNewBuffer(long offset) throws IOException {
		ByteBuffer ret = fileChannel.map(mapMode, offset, sliceSize);
		if (mapMode == FileChannel.MapMode.READ_ONLY) {
			ret = ret.asReadOnlyBuffer();
		}
		return ret;
	}

	/*
	@Override
	public void seek(long position) throws IOException {
		super.seek(position);
		this.fileChannel.position(position);
	}

	@Override
	public long getPosition() throws IOException {
		return this.fileChannel.position();
	}
*/

	@Override
	public long getLength() throws IOException {
		return this.fileChannel.size();
	}

	@Override
	public long getPositionLimit() {
		return -1;
	}

	@Override
	public boolean isClosed() {
		return !this.fileChannel.isOpen();
	}

	@Override
	public void close() throws IOException {
		growLock.lock();
		try {
			fileChannel.close();
			raf.close();

			for (ByteBuffer b : slices) {
				if (b != null && (b instanceof MappedByteBuffer)) {
					unmap((MappedByteBuffer) b);
				}
			}

			slices = null;

		} finally {
			growLock.unlock();
		}
	}

	@Override
	public void clear(long startOffset, long endOffset) throws IOException {
		super.clear(startOffset, endOffset);
	}
}
