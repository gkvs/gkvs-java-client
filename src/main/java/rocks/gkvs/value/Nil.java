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
	public boolean isNil() {
		return true;
	}
	
	@Override
	public Bool asBool(Bool defaultValue) {
		return defaultValue;
	}
	
	@Override
	public Num asNum(Num defaultValue) {
		return defaultValue;
	}
	
	@Override
	public Str asStr(Str defaultValue) {
		return defaultValue;
	}
	
	@Override
	public Table asTable(Table defaultTable) {
		return defaultTable;
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
