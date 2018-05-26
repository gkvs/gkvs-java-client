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

/**
 *  GKVS singleton class is visible from scala 
 */

public enum GKVS {

	Client;
	
	public Get get(String tableName, String recordKey) {
		return GKVSClient.getDefaultInstance().get(tableName, recordKey);
	}
	
	public Get get(Key key) {
		return GKVSClient.getDefaultInstance().get(key);
	}
	
	public static MultiGet multiGet(String tableName, String... recordKey) {
		return GKVSClient.getDefaultInstance().multiGet(tableName, recordKey);
	}
	
	public static Put put(String tableName, String recordKey) {
		return GKVSClient.getDefaultInstance().put(tableName, recordKey);
	}
	
	public static Remove remove(String tableName, String recordKey) {
		return GKVSClient.getDefaultInstance().remove(tableName, recordKey);
	}

	
}
