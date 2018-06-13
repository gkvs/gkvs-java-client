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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class KeyValue {

	private final Key key;
	private final Iterable<Value> cells;
	
	public KeyValue(Key key, Iterable<Value> cells) {
		
		if (key == null) {
			throw new IllegalArgumentException("key is null");
		}

		if (cells == null) {
			throw new IllegalArgumentException("cells is null");
		}

		this.key = key;
		this.cells = cells;
	}
	
	public static KeyValue of(Key key, Iterable<Value> cells) {
		return new KeyValue(key, cells);
	}
	
	public KeyValue(Key key, Value value) {
		
		if (key == null) {
			throw new IllegalArgumentException("key is null");
		}

		if (value == null) {
			throw new IllegalArgumentException("value is null");
		}

		this.key = key;
		this.cells = Collections.singleton(value);
	}
	
	public static KeyValue of(Key key, Value value) {
		return new KeyValue(key, value);
	}
	
	public KeyValue(Key key, Value... value) {
		
		if (key == null) {
			throw new IllegalArgumentException("key is null");
		}

		if (value == null) {
			throw new IllegalArgumentException("value is null");
		}

		this.key = key;
		
		List<Value> list = new ArrayList<Value>(value.length);
		for (Value v : value) {
			list.add(v);
		}
		
		this.cells = list;
		
	}
	
	public static KeyValue of(Key key, Value... value) {
		return new KeyValue(key, value);
	}
	
	public Key key() {
		return key;
	}
	
	public Iterable<Value> cells() {
		return cells;
	}

	@Override
	public String toString() {
		return "KeyValue [key=" + key + "]";
	}

	
}
