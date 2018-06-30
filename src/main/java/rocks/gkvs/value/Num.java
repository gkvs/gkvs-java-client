/*
 *
 * Copyright 2018-present GKVS authors.
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
package rocks.gkvs.value;

import java.io.IOException;

import javax.annotation.Nullable;

import org.msgpack.core.MessagePacker;
import org.msgpack.value.impl.ImmutableDoubleValueImpl;
import org.msgpack.value.impl.ImmutableLongValueImpl;

/**
 * 
 * Num
 *
 * @author Alex Shvid
 * @date Jun 27, 2018 
 *
 */

public final class Num extends Value {

	private final long longValue;
	private final double doubleValue;
	private final NumType type;

	public Num(long value) {
		this.type = NumType.INT64;
		this.longValue = value;
		this.doubleValue = 0.0;
	}

	public Num(double value) {
		this.type = NumType.FLOAT64;
		this.longValue = 0L;
		this.doubleValue = value;
	}

	public Num(String stringValue) {

		if (stringValue == null) {
			throw new IllegalArgumentException("value is null");
		}

		this.type = detectNumber(stringValue);

		if (this.type == null) {
			throw new IllegalArgumentException("NAN: " + stringValue);
		}

		switch (this.type) {
		case INT64:
			this.longValue = Long.parseLong(stringValue);
			this.doubleValue = 0.0;
			break;
		case FLOAT64:
			this.longValue = 0L;
			this.doubleValue = Double.parseDouble(stringValue);
			break;
		default:
			throw new IllegalStateException("unknown type: " + stringValue);
		}

	}

	public NumType getType() {
		return type;
	}
	
	@Override
	public boolean isNil() {
		return false;
	}

	@Override
	public Bool asBool(Bool defaultValue) {
		return new Bool(asLong() > 0L);
	}
	
	@Override
	public Num asNum(Num defaultValue) {
		return this;
	}
	
	@Override
	public Str asStr(Str defaultValue) {
		return new Str(asString());
	}
	
	@Override
	public Table asTable(Table defaultTable) {
		return defaultTable;
	}

	public Num add(Num otherNumber) {
		switch (otherNumber.getType()) {
		case INT64:
			return add(otherNumber.asLong());
		case FLOAT64:
			return add(otherNumber.asDouble());
		}
		throw new IllegalStateException("unexpected type: " + otherNumber.getType());
	}

	public Num add(long otherLongValue) {
		switch (type) {
		case INT64:
			return new Num(longValue + otherLongValue);
		case FLOAT64:
			return new Num(doubleValue + (double) otherLongValue);
		}
		throw new IllegalStateException("unexpected type: " + type);
	}

	public Num add(double otherDoubleValue) {
		switch (type) {
		case INT64:
			return new Num((double) longValue + otherDoubleValue);
		case FLOAT64:
			return new Num(doubleValue + otherDoubleValue);
		}
		throw new IllegalStateException("unexpected type: " + type);
	}

	public Num subtract(Num otherNumber) {
		switch (otherNumber.getType()) {
		case INT64:
			return subtract(otherNumber.asLong());
		case FLOAT64:
			return subtract(otherNumber.asDouble());
		}
		throw new IllegalStateException("unexpected type: " + otherNumber.getType());
	}

	public Num subtract(long otherLongValue) {
		switch (type) {
		case INT64:
			return new Num(longValue - otherLongValue);
		case FLOAT64:
			return new Num(doubleValue - (double) otherLongValue);
		}
		throw new IllegalStateException("unexpected type: " + type);
	}

	public Num subtract(double otherDoubleValue) {
		switch (type) {
		case INT64:
			return new Num((double) longValue - otherDoubleValue);
		case FLOAT64:
			return new Num(doubleValue - otherDoubleValue);
		}
		throw new IllegalStateException("unexpected type: " + type);
	}

	public long asLong() {
		switch (type) {
		case INT64:
			return longValue;
		case FLOAT64:
			return (long) doubleValue;
		}
		throw new IllegalStateException("unexpected type: " + type);
	}

	public double asDouble() {
		switch (type) {
		case INT64:
			return longValue;
		case FLOAT64:
			return doubleValue;
		}
		throw new IllegalStateException("unexpected type: " + type);
	}

	public String asString() {
		switch (type) {
		case INT64:
			return Long.toString(longValue);
		case FLOAT64:
			return Double.toString(doubleValue);
		}
		throw new IllegalStateException("unexpected type: " + type);
	}

	@Override
	public org.msgpack.value.Value toMsgpackValue() {
		switch (type) {
		case INT64:
			return new ImmutableLongValueImpl(longValue);
		case FLOAT64:
			return new ImmutableDoubleValueImpl(doubleValue);
		}
		throw new IllegalStateException("unexpected type: " + type);
	}

	@Override
	public void writeTo(MessagePacker packer) throws IOException {
		switch (type) {
		case INT64:
			packer.packLong(longValue);
			break;
		case FLOAT64:
			packer.packDouble(doubleValue);
			break;
		default:
			throw new IOException("unexpected type: " + type);
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		if (type == NumType.FLOAT64) {
			long temp;
			temp = Double.doubleToLongBits(doubleValue);
			result = prime * result + (int) (temp ^ (temp >>> 32));
		} else if (type == NumType.INT64) {
			result = prime * result + (int) (longValue ^ (longValue >>> 32));
		}
		result = prime * result + ((type == null) ? 0 : type.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Num other = (Num) obj;
		if (type != other.type)
			return false;
		if (type == NumType.FLOAT64
				&& Double.doubleToLongBits(doubleValue) != Double.doubleToLongBits(other.doubleValue))
			return false;
		if (type == NumType.INT64 && longValue != other.longValue)
			return false;
		return true;
	}

	@Override
	public void print(StringBuilder str, int initialSpaces, int tabSpaces) {
		str.append("Num [type=").append(type);
		if (type == NumType.INT64) {
			str.append(", longValue=").append(longValue);
		} else if (type == NumType.FLOAT64) {
			str.append(", doubleValue=").append(doubleValue);
		}
		str.append("]");
	}

	// null result means not a number (NAN)
	public static @Nullable NumType detectNumber(String stringValue) {

		if (stringValue == null) {
			return null;
		}

		int length = stringValue.length();
		if (length == 0) {
			return null;
		}
		boolean first = true;
		boolean haveDot = false;
		boolean haveE = false;
		for (int i = 0; i != length; ++i) {
			char ch = stringValue.charAt(i);
			if (first && ch == '-') {
				first = false;
				continue;
			}
			first = false;
			if (ch == '.') {
				if (haveDot) {
					return null;
				}
				haveDot = true;
				continue;
			}
			if (ch == 'E') {
				if (haveE) {
					return null;
				}
				haveE = true;
				continue;
			}
			if (ch >= '0' && ch <= '9') {
				continue;
			}
			return null;
		}
		return (haveDot || haveE) ? NumType.FLOAT64 : NumType.INT64;
	}

}
