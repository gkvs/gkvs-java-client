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

import rocks.gkvs.value.Value;

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

	public static final String DIGEST_PREFIX = "hash!";
	private static final byte[] DIGEST_PREFIX_BYTES = DIGEST_PREFIX.getBytes(); 
	
	private final String viewName;
	private final ByteString recordKey;

	protected Key(String viewName, ByteString recordKey) {
		
		if (viewName == null) {
			throw new IllegalArgumentException("viewName is null");
		}		

		if (recordKey == null) {
			throw new IllegalArgumentException("recordKey is null");
		}		

		this.viewName = viewName;
		this.recordKey = recordKey;
	}

	public String getViewName() {
		return viewName;
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
	
	public static Key raw(String viewName, String recordKey) {
		
		if (recordKey == null) {
			throw new IllegalArgumentException("recordKey is null");
		}
		
		return new Key(viewName, ByteString.copyFrom(recordKey, GkvsConstants.MUTABLE_KEY_CHARSET));
	}

	public static Key raw(String viewName, byte[] recordKey) {
		
		if (recordKey == null) {
			throw new IllegalArgumentException("recordKey is null");
		}
		
		return new Key(viewName, ByteString.copyFrom(recordKey));
	}

	public static Key digest(String viewName, byte[] recordKeyDigest) {
		
		if (recordKeyDigest == null) {
			throw new IllegalArgumentException("recordKeyDigest is null");
		}
		
		byte[] key = new byte[DIGEST_PREFIX_BYTES.length + recordKeyDigest.length];
		System.arraycopy(DIGEST_PREFIX_BYTES, 0, key, 0, DIGEST_PREFIX_BYTES.length);		
		System.arraycopy(recordKeyDigest, 0, key, DIGEST_PREFIX_BYTES.length, recordKeyDigest.length);
		
		return new Key(viewName, ByteString.copyFrom(key));
	}

	protected rocks.gkvs.protos.Key toProto() {
		rocks.gkvs.protos.Key.Builder b = rocks.gkvs.protos.Key.newBuilder();
		b.setViewName(viewName);
		b.setRecordKey(recordKey);
		return b.build();
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((recordKey == null) ? 0 : recordKey.hashCode());
		result = prime * result + ((viewName == null) ? 0 : viewName.hashCode());
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
		if (viewName == null) {
			if (other.viewName != null)
				return false;
		} else if (!viewName.equals(other.viewName))
			return false;
		return true;
	}

	@Override
	public String toString() {
		StringBuilder out  = new StringBuilder();
		out.append("Key [").append(viewName).append(":");
		
		int size = recordKey.size();
		for (int i = 0; i != size; ++i) {
			int b = recordKey.byteAt(i);
			if (isPrintable(b & 0xFF)) {
				out.append((char) b);
			}
			else {
				out.append("\\u00");
				out.append(Value.firstHex(b));
				out.append(Value.secondHex(b));
			}
		}
		
		out.append("]");
		return out.toString();
	}

	public static boolean isPrintable(byte[] array) {
		int size = array.length;
		for (int i = 0; i != size; ++i) {
			int b = array[i];
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
