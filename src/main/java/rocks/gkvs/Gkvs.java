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

import rocks.gkvs.value.Value;

/**
 * 
 * Gkvs
 * 
 * Singleton class is visible from scala 
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
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
	
	public Put put(String tableName, String recordKey, Value value) {
		return GkvsClient.getDefaultInstance().put(tableName, recordKey, value);
	}
	
	public Put put(Key key, Value value) {
		return GkvsClient.getDefaultInstance().put(key, value);
	}
	
	public Put put(KeyValue keyValue) {
		return GkvsClient.getDefaultInstance().put(keyValue);
	}
	
	public Put compareAndPut(String tableName, String recordKey, Value value, long version) {
		return GkvsClient.getDefaultInstance().compareAndPut(tableName, recordKey, value, version);
	}
	
	public Put compareAndPut(Key key, Value value, long version) {
		return GkvsClient.getDefaultInstance().compareAndPut(key, value, version);
	}
	
	public Put compareAndPut(KeyValue keyValue, long version) {
		return GkvsClient.getDefaultInstance().compareAndPut(keyValue, version);
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
