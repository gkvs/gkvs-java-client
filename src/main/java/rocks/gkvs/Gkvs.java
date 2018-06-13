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

import java.util.Iterator;
import java.util.Map;

/**
 *  GKVS singleton class is visible from scala 
 */

public enum Gkvs {

	Client;
	
	public Get exists(String tableName, String recordKey) {
		return GkvsClient.getDefaultInstance().exists(tableName, recordKey);
	}
	
	public Get exists(Key key) {
		return GkvsClient.getDefaultInstance().exists(key);
	}
	
	public Get get(String tableName, String recordKey) {
		return GkvsClient.getDefaultInstance().get(tableName, recordKey);
	}
	
	public Get get(Key key) {
		return GkvsClient.getDefaultInstance().get(key);
	}
	
	public MultiGet multiGet(Key...keys) {
		return GkvsClient.getDefaultInstance().multiGet(keys);
	}
	
	public MultiGet multiGet(Iterator<Key> keys) {
		return GkvsClient.getDefaultInstance().multiGet(keys);
	}
	
	public MultiGet multiGet(Iterable<Key> keys) {
		return GkvsClient.getDefaultInstance().multiGet(keys);
	}
	
	public GetAll getAll() {
		return GkvsClient.getDefaultInstance().getAll();
	}
	
	public Put putWithKey(String tableName, String recordKey) {
		return GkvsClient.getDefaultInstance().putWithKey(tableName, recordKey);
	}
	
	public Put putWithKey(Key key) {
		return GkvsClient.getDefaultInstance().putWithKey(key);
	}
	
	public Put put(String tableName, String recordKey, String value) {
		return putWithKey(tableName, recordKey).put(Value.of(value));
	}
	
	public Put put(Key key, String value) {
		return putWithKey(key).put(Value.of(value));
	}
	
	public Put put(String tableName, String recordKey, String column, String value) {
		return putWithKey(tableName, recordKey).put(Value.of(column, value));
	}
	
	public Put put(Key key, String column, String value) {
		return putWithKey(key).put(Value.of(column, value));
	}

	public Put put(String tableName, String recordKey, byte[] value) {
		return putWithKey(tableName, recordKey).put(Value.of(value));
	}
	
	public Put put(Key key, byte[] value) {
		return putWithKey(key).put(Value.of(value));
	}
	
	public Put put(String tableName, String recordKey, String column, byte[] value) {
		return putWithKey(tableName, recordKey).put(Value.of(column, value));
	}
	
	public Put put(Key key, String column, byte[] value) {
		return putWithKey(key).put(Value.of(column, value));
	}

	public Put put(Key key, Value value) {
		return putWithKey(key).put(value);
	}

	public Put put(Key key, Value... values) {
		return putWithKey(key).putAll(values);
	}

	public Put put(Key key, Iterable<Value> values) {
		return putWithKey(key).putAll(values);
	}

	public Put put(Key key, Map<String, byte[]> values) {
		return putWithKey(key).putAll(values);
	}
	
	public PutAll putAll() {
		return GkvsClient.getDefaultInstance().putAll();
	}
	
	public Remove remove(String tableName, String recordKey) {
		return GkvsClient.getDefaultInstance().remove(tableName, recordKey);
	}
	
	public Remove remove(Key key) {
		return GkvsClient.getDefaultInstance().remove(key);
	}
	
	public RemoveAll removeAll() {
		return GkvsClient.getDefaultInstance().removeAll();
	}
	
	public Scan scan(String tableName) {
		return GkvsClient.getDefaultInstance().scan(tableName);
	}
	
}
