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

import rocks.gkvs.value.Value;

/**
 * 
 * KeyValue
 * 
 * Each key-value has key and value
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public final class KeyValue {

	private final Key key;
	private final Value value;
	
	public KeyValue(Key key, Value value) {
		
		if (key == null) {
			throw new IllegalArgumentException("key is null");
		}

		if (value == null) {
			throw new IllegalArgumentException("value is null");
		}

		this.key = key;
		this.value = value;
	}
	
	public static KeyValue of(Key key, Value value) {
		return new KeyValue(key, value);
	}
	
	public Key key() {
		return key;
	}
	
	public Value value() {
		return value;
	}

	@Override
	public String toString() {
		return "KeyValue [key=" + key + ", value=" + value + "]";
	}

	
}
