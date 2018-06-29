package rocks.gkvs.value;

import java.io.IOException;

import org.msgpack.core.MessagePacker;
import org.msgpack.value.impl.ImmutableBooleanValueImpl;

/**
 * 
 * Bool
 *
 * @author Alex Shvid
 * @date Jun 27, 2018 
 *
 */

public final class Bool extends Value {

	private final boolean booleanValue;

	public Bool(boolean value) {
		this.booleanValue = value;
	}

	public Bool(int value) {
		this.booleanValue = value > 0;
	}

	public Bool(long value) {
		this.booleanValue = value > 0;
	}
	
	public Bool(String value) {
		this.booleanValue = Boolean.parseBoolean(value);
	}

	@Override
	public String asString() {
		return Boolean.toString(booleanValue);
	}

	public boolean asBoolean() {
		return booleanValue;
	}

	@Override
	public org.msgpack.value.Value toMsgpackValue() {
		return booleanValue ? ImmutableBooleanValueImpl.TRUE : ImmutableBooleanValueImpl.FALSE;
	}

	@Override
	public void writeTo(MessagePacker packer) throws IOException {
		packer.packBoolean(booleanValue);
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (booleanValue ? 1231 : 1237);
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
		Bool other = (Bool) obj;
		if (booleanValue != other.booleanValue)
			return false;
		return true;
	}

	@Override
	public void print(StringBuilder str, int initialSpaces, int tabSpaces) {
		str.append("Bool [booleanValue=").append(booleanValue).append("]");
	}

}
