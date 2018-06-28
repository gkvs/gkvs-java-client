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
package rocks.gkvs;

import java.io.IOException;
import java.io.OutputStream;

import javax.annotation.Nullable;

import rocks.gkvs.value.Bool;
import rocks.gkvs.value.Num;
import rocks.gkvs.value.Str;
import rocks.gkvs.value.Table;
import rocks.gkvs.value.Value;

/**
 * 
 * NullableValue
 * 
 * Wrapper class on top of Value
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public final class NullableValue {

	private final @Nullable rocks.gkvs.protos.Value value;
	
	protected NullableValue(rocks.gkvs.protos.Value value) {
		this.value = value;
	}

	public boolean isNull() {
		return value == null;
	}
	
	public Value get() {
		if (value != null) {
			return Transformers.fromProto(value);
		}
		return null;
	}
	
	public byte[] raw() {
		if (value == null) {
			return null;
		}
		return value.toByteArray();
	}

	public Bool asBool() {
		return Value.toBool(get());
	}

	public Num asNum() {
		return Value.toNum(get());
	}

	public Str asStr() {
		return Value.toStr(get());
	}
	
	public Table asTable() {
		return Value.toTable(get());
	}

	public void writeTo(OutputStream out) throws IOException {
		if (value == null) {
			throw new GkvsException("value is null");
		}
		value.writeTo(out);
	}

	@Override
	public String toString() {
		return String.valueOf(get());
	}

}
