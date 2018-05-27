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

import com.google.protobuf.ByteString;

public final class Value {

	private final String column;
	private final ByteString value;
	private final long timestamp;
	
	protected Value(String column, ByteString value, long timestamp) {
		this.column = column;
		this.value = value;
		this.timestamp = timestamp;
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
			throw new IllegalArgumentException("column is null");
		}

		if (value == null) {
			throw new IllegalArgumentException("value is null");
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

	public byte[] value() {
		return value.toByteArray();
	}
	
	public String valueAsString() {
		return value.toString(GKVSConstants.MUTABLE_VALUE_CHARSET);
	}

	public long timestamp() {
		return timestamp;
	}
	
	protected ByteString valueBytes() {
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
