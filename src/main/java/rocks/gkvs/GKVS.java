/*
 *
 * Copyright 2018 gKVS authors.
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

import java.util.Map;

/**
 *  GKVS singleton class is visible from scala 
 */

public enum GKVS {

	Client;
	
	public Get exists(String tableName, String recordKey) {
		return GKVSClient.getDefaultInstance().exists(tableName, recordKey);
	}
	
	public Get exists(Key key) {
		return GKVSClient.getDefaultInstance().exists(key);
	}
	
	public Get get(String tableName, String recordKey) {
		return GKVSClient.getDefaultInstance().get(tableName, recordKey);
	}
	
	public Get get(Key key) {
		return GKVSClient.getDefaultInstance().get(key);
	}
	
	public MultiGet multiGet(Key...keys) {
		return GKVSClient.getDefaultInstance().multiGet(keys);
	}
	
	public Put putWithKey(String tableName, String recordKey) {
		return GKVSClient.getDefaultInstance().putWithKey(tableName, recordKey);
	}
	
	public Put putWithKey(Key key) {
		return GKVSClient.getDefaultInstance().putWithKey(key);
	}
	
	public Put put(String tableName, String recordKey, String value) {
		return GKVSClient.getDefaultInstance().putWithKey(tableName, recordKey).put(Value.of(value));
	}

	public Put put(String tableName, String recordKey, byte[] value) {
		return GKVSClient.getDefaultInstance().putWithKey(tableName, recordKey).put(Value.of(value));
	}

	public Put put(String tableName, String recordKey, Value value) {
		return GKVSClient.getDefaultInstance().putWithKey(tableName, recordKey).put(value);
	}

	public Put put(String tableName, String recordKey, Value... values) {
		return GKVSClient.getDefaultInstance().putWithKey(tableName, recordKey).putAll(values);
	}

	public Put put(String tableName, String recordKey, Iterable<Value> cells) {
		return GKVSClient.getDefaultInstance().putWithKey(tableName, recordKey).putAll(cells);
	}

	public Put put(String tableName, String recordKey, Map<String, byte[]> map) {
		return GKVSClient.getDefaultInstance().putWithKey(tableName, recordKey).putAll(map);
	}
	
	public Remove remove(String tableName, String recordKey) {
		return GKVSClient.getDefaultInstance().remove(tableName, recordKey);
	}
	
	public Remove remove(Key key) {
		return GKVSClient.getDefaultInstance().remove(key);
	}
	
}
