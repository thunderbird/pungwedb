package com.pungwe.db.io.serializers;

import com.pungwe.db.constants.TypeReference;
import com.pungwe.db.types.DBDocument;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ian on 24/10/2014.
 */
public class Serializers {

	public static <T> Serializer<T> serializerForType(T type) {
		if (type == null) {
			return new NULL();
		}

		if (type instanceof String) {
			return (Serializer<T>)new STRING();
		}

		if (type instanceof Double || type instanceof Float) {
			return (Serializer<T>)new DECIMAL();
		}

		if (type instanceof Number) {
			return (Serializer<T>)new STRING();
		}

		if (type instanceof BOOLEAN) {
			return (Serializer<T>)new BOOLEAN();
		}

		if (type instanceof byte[] || type instanceof Byte[]) {
			return (Serializer<T>)new BYTES();
		}

		if (type instanceof DBDocument) {
			return (Serializer<T>)new DBDocumentSerializer();
		}

		throw new IllegalArgumentException("Unsupported type: " + type.getClass().getName());
	}

	public static class NULL implements Serializer {

		public void serialize(DataOutput out, Object value) {
		}

		@Override
		public Object deserialize(DataInput in) throws IOException {
			return null;
		}

		@Override
		public TypeReference getTypeReference() {
			return TypeReference.NULL;
		}
	}

	public static class STRING implements Serializer<String> {

		@Override
		public void serialize(DataOutput out, String value) throws IOException {
			out.writeUTF(value);
		}

		@Override
		public String deserialize(DataInput in) throws IOException {
			return in.readUTF();
		}

		@Override
		public TypeReference getTypeReference() {
			return TypeReference.STRING;
		}
	}

	public static class NUMBER implements Serializer<Long> {
		@Override
		public void serialize(DataOutput out, Long value) throws IOException {
			out.writeLong(value);
		}

		@Override
		public Long deserialize(DataInput in) throws IOException {
			return in.readLong();
		}

		@Override
		public TypeReference getTypeReference() {
			return TypeReference.NUMBER;
		}
	}

	public static class DECIMAL implements Serializer<Double> {
		@Override
		public void serialize(DataOutput out, Double value) throws IOException {
			out.writeDouble(value);
		}

		@Override
		public Double deserialize(DataInput in) throws IOException {
			return in.readDouble();
		}

		@Override
		public TypeReference getTypeReference() {
			return TypeReference.DECIMAL;
		}
	}

	public static class BOOLEAN implements Serializer<Boolean> {

		@Override
		public void serialize(DataOutput out, Boolean value) throws IOException {
			out.writeBoolean(value);
		}

		@Override
		public Boolean deserialize(DataInput in) throws IOException {
			return in.readBoolean();
		}

		@Override
		public TypeReference getTypeReference() {
			return TypeReference.BOOLEAN;
		}
	}

	public static class BYTES implements Serializer<byte[]> {

		@Override
		public void serialize(DataOutput out, byte[] value) throws IOException {
			out.writeInt(value.length);
			out.write(value);
		}

		@Override
		public byte[] deserialize(DataInput in) throws IOException {
			int size = in.readInt();
			byte[] b = new byte[size];
			in.readFully(b);
			return b;
		}

		@Override
		public TypeReference getTypeReference() {
			return TypeReference.BINARY;
		}
	}
}
