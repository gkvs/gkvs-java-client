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
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.msgpack.core.MessagePacker;
import org.msgpack.value.impl.ImmutableBinaryValueImpl;
import org.msgpack.value.impl.ImmutableStringValueImpl;


/**
 * 
 * Str
 *
 * @author Alex Shvid
 * @date Jun 27, 2018 
 *
 */

public final class Str extends Value {

	private final String stringValue;
	private final byte[] bytesValue;
	private final int offset;
	private final int length;
	private final StrType type;
	
	public Str(String value) {
		
		if (value == null) {
			throw new IllegalArgumentException("empty value");
		}		
		
		this.stringValue = value;
		this.bytesValue = null;
		this.offset = 0;
		this.length = 0;
		this.type = StrType.UTF8;
	}
	
	public Str(byte[] value, boolean copy) {
		this(value, 0, value != null ? value.length : 0, copy);
	}
	
	public Str(byte[] value, int offset, int length, boolean copy) {
		
		if (value == null) {
			throw new IllegalArgumentException("empty value");
		}
		
		this.stringValue = null;
		
		if (copy) {
			this.bytesValue = copyOf(value, offset, length);
			this.offset = 0;
			this.length = this.bytesValue.length;
		}
		else {
			this.bytesValue = value;
			this.offset = offset;
			this.length = length;
		}
		
		this.type = StrType.RAW;
	}
	
	private static byte[] copyOf(byte[] src, int offset, int length) {
        byte[] copy = new byte[length];
        System.arraycopy(src, offset, copy, 0,
                         Math.min(src.length - offset, length));
        return copy;
	}
	
	public StrType getType() {
		return type;
	}
	
	@Override
	public boolean isNil() {
		return false;
	}

	@Override
	public Bool asBool(Bool defaultValue) {
		return new Bool(Boolean.parseBoolean(asString()));
	}
	
	@Override
	public Num asNum(Num defaultValue) {
		try {
			return new Num(asString());
		}
		catch(IllegalArgumentException e) {
			throw new ParseException(asString(), e);
		}
	}
	
	@Override
	public Str asStr(Str defaultValue) {
		return this;
	}
	
	@Override
	public Table asTable(Table defaultTable) {
		return defaultTable;
	}
	
	@Override
	public String asString() {
		return asUtf8();
	}
	
	public String asUtf8() {
		
		switch(type) {
		
		case UTF8:
			return stringValue;
			
		case RAW:
			return new String(bytesValue, StandardCharsets.UTF_8);
			
		}
		
		throw new IllegalStateException("unexpected type: " + type);
		
	}
	
	public byte[] asBytes() {
		
		switch(type) {
		
		case UTF8:
			return stringValue.getBytes(StandardCharsets.UTF_8);
			
		case RAW:
			return bytesValue;
			
		}
		
		throw new IllegalStateException("unexpected type: " + type);
	}
	
	@Override
	public org.msgpack.value.Value toMsgpackValue() {
		
		switch(type) {
		
		case UTF8:
	    return new ImmutableStringValueImpl(stringValue);  
		case RAW:
			return new ImmutableBinaryValueImpl(bytesValue);  
		}
		
		throw new IllegalStateException("unexpected type: " + type);
	}

  @Override
	public void writeTo(MessagePacker packer) throws IOException {
		switch(type) {
		
		case UTF8:
			byte[] data = stringValue.getBytes(StandardCharsets.UTF_8);
			packer.packRawStringHeader(data.length);
			packer.writePayload(data);
			break;
			
		case RAW:
			packer.packBinaryHeader(length);
			packer.writePayload(bytesValue, offset, length);
			break;
			
		default:
		  throw new IOException("unexpected type: " + type);		
		}	
	}
  
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		if (type == StrType.RAW) {
			result = prime * result + Arrays.hashCode(bytesValue);
		}
		if (type == StrType.UTF8) {
			result = prime * result + ((stringValue == null) ? 0 : stringValue.hashCode());
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
		Str other = (Str) obj;
		if (type != other.type)
			return false;
		if (type == StrType.RAW && !Arrays.equals(bytesValue, other.bytesValue))
			return false;
		if (type == StrType.UTF8) {
			if (stringValue == null) {
				if (other.stringValue != null)
					return false;
			} else if (!stringValue.equals(other.stringValue))
				return false;
		}
		return true;
	}

	@Override
	public void print(StringBuilder str, int initialSpaces, int tabSpaces) {
		str.append("Str [type=").append(type);
		if (type == StrType.UTF8) {
			str.append(", stringValue=").append(stringValue);
		}
		else if (type == StrType.RAW) {
			str.append(", bytesValue=").append(Arrays.toString(bytesValue));
		}
		str.append("]");
	}
	
}
