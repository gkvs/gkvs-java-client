package rocks.gkvs.value;

import java.io.IOException;

import org.msgpack.core.MessagePacker;
import org.msgpack.value.impl.ImmutableNilValueImpl;

/**
 * 
 * Nil
 *
 * @author Alex Shvid
 * @date Jun 29, 2018 
 *
 */

public final class Nil extends Value {

	private final static Nil instance = new Nil();
	
	private Nil() {
	}
	
	public static Nil get() {
		return instance;
	}
	
	@Override
	public String asString() {
		return "";
	}
	
	@Override
	public org.msgpack.value.Value toMsgpackValue() {
		return ImmutableNilValueImpl.get();
	}

	@Override
	public void writeTo(MessagePacker packer) throws IOException {
		packer.packNil();
	}
	
	@Override
	public void print(StringBuilder str, int initialSpaces, int tabSpaces) {
		str.append("Nil");
	}

	
}
