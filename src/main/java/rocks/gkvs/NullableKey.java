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

import javax.annotation.Nullable;

/**
 * 
 * NullableKey
 *
 * Wrapper class on top of Key
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public final class NullableKey {

	private final @Nullable Key key;
	
	protected NullableKey(Key key) {
		this.key = key;
	}
	
	public boolean isNull() {
		return key == null;
	}
	
	public Key get() {
		return key;
	}
	
	public String getTableName() {
		return key != null ? key.getStoreName() : null;
	}

	public KeyType getRecordKeyType() {
		return key != null ? key.getRecordKeyType() : null;
	}

	public byte[] getRecordKeyBytes() {
		return key != null ? key.getRecordKeyBytes() : null;
	}
	
	public String getRecordKeyString() {
		return key != null ? key.getRecordKeyString() : null;
	}
	
	public String getRecordKeyHexString() {
		return key != null ? key.getRecordKeyHexString() : null;
	}
	
	public String getRecordKeyBase64String() {
		return key != null ? key.getRecordKeyBase64String() : null;
	}
	
	@Override
	public String toString() {
		return key != null ? key.toString() : "NULL";
	}
	
}
