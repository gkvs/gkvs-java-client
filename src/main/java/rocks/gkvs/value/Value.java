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
import java.io.OutputStream;

import javax.annotation.Nullable;

import org.msgpack.core.MessagePack;
import org.msgpack.core.MessagePacker;
import org.msgpack.core.buffer.ArrayBufferOutput;

import rocks.gkvs.GkvsException;
import rocks.gkvs.value.Table.NullTable;


/**
 * 
 * Value
 *
 * @author Alex Shvid
 * @date Jun 27, 2018 
 *
 */

public abstract class Value {

	private static volatile Bool DEFAULT_BOOL = new Bool(false); 
	private static volatile Num DEFAULT_NUM = new Num(0L); 
	private static volatile Str DEFAULT_STR = new Str(""); 
	private static volatile Table DEFAULT_TABLE = new Table(NullTable.NULL); 
	
	public abstract String asString();
	
	public abstract org.msgpack.value.Value toMsgpackValue();
	
	public abstract void writeTo(MessagePacker packer) throws IOException;
		
	public abstract void print(StringBuilder out, int initialSpaces, int tabSpaces);
	
	public abstract boolean isNil();
	
	public static Bool getDefaultBool() {
		return DEFAULT_BOOL;
	}
	
	public static void setDefaultBool(Bool b) {
		DEFAULT_BOOL = b;
	}

	public static Num getDefaultNum() {
		return DEFAULT_NUM;
	}
	
	public static void setDefaultNum(Num n) {
		DEFAULT_NUM = n;
	}

	public static Str getDefaultStr() {
		return DEFAULT_STR;
	}
	
	public static void setDefaultStr(Str s) {
		DEFAULT_STR = s;
	}

	public static Table getDefaultTable() {
		return DEFAULT_TABLE;
	}
	
	public static void setDefaultTable(Table t) {
		DEFAULT_TABLE = t;
	}

	public @Nullable Bool asBool() {
		return asBool(DEFAULT_BOOL);
	}
	
	public abstract Bool asBool(Bool defaultValue);

	public @Nullable Num asNum() {
		return asNum(DEFAULT_NUM);
	}
	
	public abstract Num asNum(Num defaultValue);

	public @Nullable Str asStr() {
		return asStr(DEFAULT_STR);
	}
	
	public abstract Str asStr(Str defaultValue);
	
	public @Nullable Table asTable() {
		return asTable(DEFAULT_TABLE);
	}
	
	public abstract Table asTable(Table defaultValue);
	
	public byte[] toMsgpack() {

		ArrayBufferOutput out = new ArrayBufferOutput();
		try {
			MessagePacker packer = MessagePack.newDefaultPacker(out);
			writeTo(packer);
			packer.flush();
		} catch (IOException e) {
			throw new GkvsException("i/o error", e);
		}

		return out.toByteArray();
	}
	
	public void writeTo(OutputStream out) throws IOException {
		MessagePacker packer = MessagePack.newDefaultPacker(out);
		writeTo(packer);
		packer.flush();
	
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

}
