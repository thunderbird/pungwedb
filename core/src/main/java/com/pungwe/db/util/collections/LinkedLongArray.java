package com.pungwe.db.util.collections;

import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.io.serializers.Serializer;
import com.pungwe.db.io.serializers.Serializers;
import com.pungwe.db.io.store.Store;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Iterator;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Created by 917903 on 24/03/2015.
 */
public class LinkedLongArray extends AbstractLargeCollection<Long> {

	private final AtomicLong headRecId = new AtomicLong(-1);
	private final AtomicLong size = new AtomicLong();
	private final AtomicLong nextRecordId = new AtomicLong(-1), previousRecordId = new AtomicLong(-1);
	private final Store store;
	private final ListNodeSerializer serializer = new ListNodeSerializer();

	public LinkedLongArray(Store store) throws IOException {
		this.store = store;
		nextRecordId.set(store.getNextId());
	}

	public LinkedLongArray(Store store, long headRecId, long size) throws IOException {
		this.store = store;
		this.size.set(size);
		this.headRecId.set(headRecId);
		this.nextRecordId.set(store.getNextId());
	}

	public Long get(long index) {
		Iterator<Long> it = iterator();
		long i = 0;
		while (it.hasNext()) {
			long n = it.next();
			if (i++ == index) {
				return n;
			}
		}
		throw new IndexOutOfBoundsException("No index position of: " + index);
	}

	@Override
	public boolean add(Long aLong) {
		try {
			final long recordId = nextRecordId.getAndSet(store.getNextId());
			final long previousId = previousRecordId.getAndSet(recordId);
			final long nextId = nextRecordId.get();

			ListNode node = new ListNode();
			node.removed = false;
			node.previous = previousId;
			node.next = nextId;
			node.value = aLong;
			store.update(recordId, node, serializer);
			size.incrementAndGet();
			if (headRecId.get() == -1) {
				headRecId.set(recordId);
			}
		} catch (Exception ex) {
			throw new RuntimeException(ex);
		}
		return true;
	}

	@Override
	public boolean remove(Object o) {
		if (!(o instanceof Number)) {
			throw new IllegalArgumentException("Must be a number");
		}
		Iterator<Long> it = iterator();
		while (it.hasNext()) {
			Long l = it.next();
			if (l.equals(o)) {
				it.remove();
				size.decrementAndGet();
				return true;
			}
		}
		return false;
	}

	@Override
	public Iterator<Long> iterator() {
		return new LongArrayIterator(this);
	}

	@Override
	public long sizeLong() {
		return size.get();
	}

	@Override
	public Object[] toArray(long from, boolean fromInclusive, long to, boolean toInclusive) {
		if (to - from > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Range too big for a java array, must be less than: " + Integer.MAX_VALUE + " elements");
		}
		long start = fromInclusive ? from : (from + 1);
		long end = toInclusive ? to + 1 : to;

		Long[] array = new Long[(int) (end - start)];
		Iterator<Long> it = iterator();
		int i = 0;
		while (it.hasNext()) {
			array[i++] = it.next();
		}
		return array;
	}

	private static class LongArrayIterator implements Iterator<Long> {

		private final LinkedLongArray array;
		private final AtomicLong position = new AtomicLong();
		private volatile ListNode currentNode;
		private volatile long currentNodeId;

		public LongArrayIterator(LinkedLongArray array) {
			this.array = array;
		}

		private void pointToStart() {
			try {
				if (array.headRecId.get() == -1) {
					return;
				}
				currentNodeId = array.headRecId.get();
				currentNode = array.store.get(array.headRecId.get(), array.serializer);
			} catch (Exception ex) {
				throw new RuntimeException(ex);
			}
		}

		@Override
		public boolean hasNext() {
			return position.get() < array.sizeLong();
		}

		@Override
		public Long next() {
			try {
				if (!hasNext()) {
					return null;
				}
				if (currentNode == null) {
					pointToStart();
				} else {
					currentNodeId = currentNode.next;
					currentNode = array.store.get(currentNodeId, array.serializer);
				}
				if (currentNode.removed) {
					return next();
				}
				if (currentNode == null) {
					return next();
				}
				position.getAndIncrement();
				return currentNode.value;
			} catch (IOException ex) {
				return null;
			}
		}

		@Override
		public void remove() {
			currentNode.removed = true;
			try {
				array.store.update(currentNodeId, currentNode, array.serializer);
			} catch (Exception ex) {

			}
		}
	}

	private static final class ListNode {
		boolean removed = false;
		long value;
		long next;
		long previous;
	}

	private static final class ListNodeSerializer implements Serializer<ListNode> {
		@Override
		public void serialize(DataOutput out, ListNode value) throws IOException {
			out.writeBoolean(value.removed);
			out.writeLong(value.previous);
			out.writeLong(value.value);
			out.writeLong(value.next);
		}

		@Override
		public ListNode deserialize(DataInput in) throws IOException {
			ListNode value = new ListNode();
			value.removed = in.readBoolean();
			value.previous = in.readLong();
			value.value = in.readLong();
			value.next = in.readLong();
			return value;
		}

		@Override
		public TypeReference getTypeReference() {
			return TypeReference.OBJECT;
		}
	}
}
