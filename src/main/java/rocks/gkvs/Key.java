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

import com.google.common.io.BaseEncoding;
import com.google.protobuf.ByteString;

/**
 * 
 * Key
 * 
 * Immutable GKVS key
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public final class Key {

	private final String storeName;
	private final KeyType recordKeyType;
	private final ByteString recordKey;

	public Key(String storeName, KeyType recordKeyType, ByteString recordKey) {
		
		if (storeName == null) {
			throw new IllegalArgumentException("storeName is null");
		}		

		if (recordKeyType == null) {
			throw new IllegalArgumentException("recordKeyType is null");
		}		

		if (recordKey == null) {
			throw new IllegalArgumentException("recordKey is null");
		}		

		this.storeName = storeName;
		this.recordKeyType = recordKeyType;
		this.recordKey = recordKey;
	}

	public String getStoreName() {
		return storeName;
	}

	public KeyType getRecordKeyType() {
		return recordKeyType;
	}

	protected ByteString getRecordKey() {
		return recordKey;
	}
	
	public byte[] getRecordKeyBytes() {
		return recordKey.toByteArray();
	}
	
	public String getRecordKeyString() {
		return new String(recordKey.toByteArray(), GkvsConstants.MUTABLE_KEY_CHARSET);
	}

	public String getRecordKeyHexString() {
		return BaseEncoding.base16().upperCase().encode(recordKey.toByteArray());
	}
	
	public String getRecordKeyBase64String() {
		return BaseEncoding.base64().encode(recordKey.toByteArray());
	}
	
	public static Key raw(String storeName, String recordKey) {
		return new Key(storeName, KeyType.RAW, ByteString.copyFrom(recordKey, GkvsConstants.MUTABLE_KEY_CHARSET));
	}

	public static Key raw(String storeName, byte[] recordKey) {
		return new Key(storeName, KeyType.RAW, ByteString.copyFrom(recordKey));
	}

	public static Key digest(String storeName, byte[] recordKeyDigest) {
		return new Key(storeName, KeyType.RAW, ByteString.copyFrom(recordKeyDigest));
	}

	protected rocks.gkvs.protos.Key toProto() {
		rocks.gkvs.protos.Key.Builder b = rocks.gkvs.protos.Key.newBuilder();
		b.setStoreName(storeName);
		switch (recordKeyType) {
		case RAW:
			b.setRaw(recordKey);
			break;
		case DIGEST:
			b.setDigest(recordKey);
			break;
		}
		return b.build();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((recordKey == null) ? 0 : recordKey.hashCode());
		result = prime * result + ((recordKeyType == null) ? 0 : recordKeyType.hashCode());
		result = prime * result + ((storeName == null) ? 0 : storeName.hashCode());
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
		Key other = (Key) obj;
		if (recordKey == null) {
			if (other.recordKey != null)
				return false;
		} else if (!recordKey.equals(other.recordKey))
			return false;
		if (recordKeyType != other.recordKeyType)
			return false;
		if (storeName == null) {
			if (other.storeName != null)
				return false;
		} else if (!storeName.equals(other.storeName))
			return false;
		return true;
	}

	@Override
	public String toString() {
		if (isPrintableKey()) {
			return "Key [" + storeName + ":" + recordKeyType.name() + ":" + getRecordKeyString() + "]";
		}
		else {
			return "Key [" + storeName + ":" + recordKeyType.name() + ":BASE64:" + getRecordKeyBase64String() + "]";
		}
	}

	private boolean isPrintableKey() {
		if (recordKeyType == KeyType.DIGEST) {
			return false;
		}
		int size = recordKey.size();
		for (int i = 0; i != size; ++i) {
			int b = recordKey.byteAt(i);
			if (!isPrintable(b & 0xFF)) {
				return false;
			}
		}
		return true;
	}
	
	public static boolean isPrintable(int c) {
		return c > 31 && c < 127;
	}
	
}
