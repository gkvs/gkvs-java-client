/*
 *
 * Copyright 2018 gKVS authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package rocks.gkvs;

import java.io.IOException;
import java.io.OutputStream;

import com.google.protobuf.ByteString;

public final class Value implements Valuable {

	private final String column;
	private final ByteString value;
	private final long timestamp;
	
	protected Value(rocks.gkvs.protos.Value value) {
		this.column = value.getColumn();
		this.value = getValuePayload(value);
		this.timestamp = value.getTimestamp();
	}
	
	private static ByteString getValuePayload(rocks.gkvs.protos.Value value) {

		switch (value.getValueCase()) {
		case RAW:
			return value.getRaw();
		case DIGEST:
			return value.getDigest();
		default:
			return ByteString.EMPTY;
		}
	}
	
	public Value(byte[] value) {
		this(GKVSConstants.DEFAULT_SINGLE_VALUE_COLUMN, value, 0L);
	}
	
	public static Value of(byte[] value) {
		return new Value(value);
	}
	
	public Value(String column, byte[] value) {
		this(column, value, 0L);
	}

	public static Value of(String column, byte[] value) {
		return new Value(column, value);
	}
	
	public Value(String column, byte[] value, long timestamp) {
		
		if (column == null) {
			throw new IllegalArgumentException("null columns are not allowed");
		}

		if (value == null) {
			throw new IllegalArgumentException("null values are not allowed");
		}

		this.column = column;
		this.value = ByteString.copyFrom(value);
		this.timestamp = timestamp;
	}
	
	public static Value of(String column, byte[] value, long timestamp) {
		return new Value(column, value, timestamp);
	}
	
	public Value(String value) {
		this(GKVSConstants.DEFAULT_SINGLE_VALUE_COLUMN, value, 0L);
	}
	
	public static Value of(String value) {
		return new Value(value);
	}
	
	public Value(String column, String value) {
		this(column, value, 0L);
	}

	public static Value of(String column, String value) {
		return new Value(column, value);
	}
	
	public Value(String column, String value, long timestamp) {
		
		if (column == null) {
			throw new IllegalArgumentException("column is null");
		}

		if (value == null) {
			throw new IllegalArgumentException("value is null");
		}
		
		this.column = column;
		this.value = ByteString.copyFrom(value, GKVSConstants.MUTABLE_VALUE_CHARSET);
		this.timestamp = timestamp;
	}
	
	public static Value of(String column, String value, long timestamp) {
		return new Value(column, value, timestamp);
	}
	
	public String column() {
		return column;
	}

	public byte[] bytes() {
		return value.toByteArray();
	}
	
	public String string() {
		return value.toString(GKVSConstants.MUTABLE_VALUE_CHARSET);
	}
	
	public void writeTo(OutputStream out) throws IOException {
		value.writeTo(out);
	}

	public long timestamp() {
		return timestamp;
	}
	
	protected ByteString byteString() {
		return value;
	}

	protected rocks.gkvs.protos.Value toProto() {
		rocks.gkvs.protos.Value.Builder builder = rocks.gkvs.protos.Value.newBuilder();
		builder.setColumn(column);
		builder.setRaw(value);
		if (timestamp > 0) {
			builder.setTimestamp(timestamp);
		}
		return builder.build();
	}
	
	@Override
	public String toString() {
		return "Value [column=" + column + ", value=" + value + ", timestamp=" + timestamp + "]";
	}

}
