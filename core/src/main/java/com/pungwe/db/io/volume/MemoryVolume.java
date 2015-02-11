package com.pungwe.db.io.volume;

import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Created by 917903 on 11/02/2015.
 */
public class MemoryVolume extends ByteBufferVolume {

	protected final boolean useDirectBuffer;
	protected volatile boolean closed = false;

	public MemoryVolume(boolean useDirectBuffer) {
		this(false, VOLUME_PAGE_SHIFT);
	}

	public MemoryVolume(boolean useDirectBuffer, int sliceShift) {
		super(false, sliceShift);
		this.useDirectBuffer = useDirectBuffer;
	}

	@Override
	public ByteBuffer makeNewBuffer(long offset) throws IOException {
		try {
			return useDirectBuffer ?
					ByteBuffer.allocateDirect(sliceSize) :
					ByteBuffer.allocate(sliceSize);
		} catch (OutOfMemoryError ex) {
			throw ex;
		}
	}

	@Override
	public long getLength() throws IOException {
		return ((long)slices.length)*sliceSize;
	}

	@Override
	public long getPositionLimit() {
		return -1;
	}

	@Override
	public boolean isClosed() {
		return closed;
	}

	@Override
	public void close() throws IOException {

	}
}
