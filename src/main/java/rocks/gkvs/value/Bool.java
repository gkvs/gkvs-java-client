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
	public boolean isNil() {
		return false;
	}

	@Override
	public Bool asBool(Bool defaultValue) {
		return this;
	}
	
	@Override
	public Num asNum(Num defaultValue) {
		return new Num(booleanValue ? 1 : 0);
	}
	
	@Override
	public Str asStr(Str defaultValue) {
		return new Str(Boolean.toString(booleanValue));
	}
	
	@Override
	public Table asTable(Table defaultTable) {
		return defaultTable;
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
