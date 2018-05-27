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

public final class Cell {

	private final String column;
	private final ByteString value;
	
	protected Cell(String column, ByteString value) {
		this.column = column;
		this.value = value;
	}

	public Cell(String column, byte[] value) {
		
		if (column == null) {
			throw new IllegalArgumentException("column is null");
		}

		if (value == null) {
			throw new IllegalArgumentException("value is null");
		}

		this.column = column;
		this.value = ByteString.copyFrom(value);
	}

	public Cell(String column, String value) {
		
		if (column == null) {
			throw new IllegalArgumentException("column is null");
		}

		if (value == null) {
			throw new IllegalArgumentException("value is null");
		}
		
		this.column = column;
		this.value = ByteString.copyFrom(value, GKVSConstants.MUTABLE_VALUE_CHARSET);
	}
	
	public String column() {
		return column;
	}

	public byte[] value() {
		return value.toByteArray();
	}

	protected ByteString valueBytes() {
		return value;
	}
	
	@Override
	public String toString() {
		return "Cell [column=" + column + ", value=" + value + "]";
	}

	
}
