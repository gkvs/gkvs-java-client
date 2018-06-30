package rocks.gkvs.value;

import java.io.IOException;
import java.io.OutputStream;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.buffer.ArrayBufferOutput;

import rocks.gkvs.GkvsException;

/**
 * 
 * Value
 *
 * @author Alex Shvid
 * @date Jun 27, 2018 
 *
 */

public abstract class Value {

	public abstract String asString();
	
	public abstract org.msgpack.value.Value toMsgpackValue();
	
	public abstract void writeTo(MessagePacker packer) throws IOException;
		
	public abstract void print(StringBuilder out, int initialSpaces, int tabSpaces);
	
	public byte[] toMsgpack() {

		ArrayBufferOutput out = new ArrayBufferOutput();
		try {
			MessagePacker packer = MessagePack.newDefaultPacker(out);
			writeTo(packer);
			packer.flush();
		} catch (IOException e) {
			throw new GkvsException("IOException happened during serialization to byte array", e);
		}

		return out.toByteArray();
	}
	
	public void writeTo(OutputStream out) {

		try {
			MessagePacker packer = MessagePack.newDefaultPacker(out);
			writeTo(packer);
			packer.flush();
		} catch (IOException e) {
			throw new GkvsException("IOException happened during serialization to output stream", e);
		}
	
	}

	public String toHexMsgpack() {
		return toHex(toMsgpack());
	}

	public String toJson() {
		return toMsgpackValue().toJson();
	}
	
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder();
		print(str, 0, 2);
		return str.toString();
	}
	
	private final static char[] HEX_ARRAY = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'a', 'b', 'c', 'd', 'e', 'f' };

	public static String toHex(byte[] bytes) {
		
		if (bytes == null) {
			return null;
		}
		
		int capacity = bytes.length << 1;
		
		char[] hexChars = new char[capacity];
		
		for (int i = 0, j = 0; i != bytes.length; ++i) {
			
			int v = bytes[i] & 0xFF;
			hexChars[j++] = HEX_ARRAY[v >>> 4];
			hexChars[j++] = HEX_ARRAY[v & 0x0F];
			
		}
		
		return new String(hexChars);
	}
	
	public static Table toTable(Value value) {
		if (value != null) {
			if (value instanceof Table) {
				return (Table) value;
			}
			throw new GkvsException("can not convert value to table: " + value);
		}
		return null;
	}

	public static Bool toBool(Value value) {
		if (value != null) {
			if (value instanceof Bool) {
				return (Bool) value;
			}
			else if (value instanceof Num) {
				return new Bool(((Num) value).asLong());
			}
			else {
				return new Bool(value.asString());
			}
		}
		return null;
	}
	
	public static Num toNum(Value value) {
		if (value != null) {
			if (value instanceof Num) {
				return (Num) value;
			}
			else {
				return new Num(value.asString());
			}
		}
		return null;
	}

	public static Str toStr(Value value) {
		if (value != null) {
			if (value instanceof Str) {
				return (Str) value;
			}
			else {
				return new Str(value.asString());
			}
		}
		return null;
	}
}
