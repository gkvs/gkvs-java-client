package rocks.gkvs.value;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import org.msgpack.core.MessagePacker;
import org.msgpack.value.impl.ImmutableBinaryValueImpl;
import org.msgpack.value.impl.ImmutableStringValueImpl;

import rocks.gkvs.GkvsException;

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
		this(value, 0, value.length, copy);
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
		
		throw new GkvsException("unexpected type: " + type);
		
	}
	
	public byte[] asBytes() {
		
		switch(type) {
		
		case UTF8:
			return stringValue.getBytes(StandardCharsets.UTF_8);
			
		case RAW:
			return bytesValue;
			
		}
		
		throw new GkvsException("unexpected type: " + type);
	}
	
	@Override
	public org.msgpack.value.Value toMsgpackValue() {
		
		switch(type) {
		
		case UTF8:
	    return new ImmutableStringValueImpl(stringValue);  
		case RAW:
			return new ImmutableBinaryValueImpl(bytesValue);  
		}
		
		throw new GkvsException("unexpected type: " + type);
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
