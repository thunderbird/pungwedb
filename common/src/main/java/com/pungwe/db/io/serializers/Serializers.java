package com.pungwe.db.io.serializers;

import com.pungwe.db.constants.TypeReference;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by ian on 24/10/2014.
 */
public interface Serializers {

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
}
