package com.pungwe.db.util.collections;

import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.io.serializers.Serializer;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by 917903 on 18/03/2015.
 */
final class DBQueueNode<E> {

	private long next;
	private long timestamp;
	private E value;

	public long getNext() {
		return next;
	}

	public void setNext(long next) {
		this.next = next;
	}

	public E getValue() {
		return value;
	}

	public void setValue(E value) {
		this.value = value;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(long timestamp) {
		this.timestamp = timestamp;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) return true;
		if (o == null || getClass() != o.getClass()) return false;

		DBQueueNode that = (DBQueueNode) o;

		if (next != that.next) return false;
		if (!value.equals(that.value)) return false;

		return true;
	}

	@Override
	public int hashCode() {
		int result = (int) (next ^ (next >>> 32));
		result = 31 * result + value.hashCode();
		return result;
	}

	public static class NodeSerializer<E1> implements Serializer<DBQueueNode<E1>> {

		private final Serializer<E1> serializer;

		public NodeSerializer(Serializer<E1> s) {
			serializer = s;
		}

		@Override
		public void serialize(DataOutput out, DBQueueNode<E1> value) throws IOException {
			out.writeLong(value.getTimestamp());
			serializer.serialize(out, value.getValue());
			out.writeLong(value.getNext());
		}

		@Override
		public DBQueueNode<E1> deserialize(DataInput in) throws IOException {
			long time = in.readLong();
			E1 value = serializer.deserialize(in);
			long next = in.readLong();
			DBQueueNode<E1> node = new DBQueueNode<>();
			node.setTimestamp(time);
			node.setValue(value);
			node.setNext(next);
			return node;
		}

		@Override
		public TypeReference getTypeReference() {
			return null;
		}
	}
}
