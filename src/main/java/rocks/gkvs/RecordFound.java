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

import rocks.gkvs.protos.ValueResult;

/**
 * 
 * RecordFound
 *
 * Record implementation
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public final class RecordFound implements Record {
	
	private final @Nullable Key requestKey;
	private final ValueResult result;
	
	protected RecordFound(@Nullable Key requestKey, ValueResult result) {
		this.requestKey = requestKey;
		this.result = result;
	}
	
	@Override
	public long requestId() {
		return result.getRequestId();
	}

	@Override
	public boolean exists() {
		return true;
	}
	
	@Override
	public long version() {
		return result.getMetadata().getVersion();
	}
	
	@Override
	public int ttl() {
		return result.getMetadata().getTtl();
	}
	
	@Override
	public NullableKey key() {
		
		if (result.hasKey()) {
			
			rocks.gkvs.protos.Key protoKey = result.getKey();
			
			switch(protoKey.getRecordKeyCase()) {
			
			case RAW:
				return new NullableKey(Key.raw(protoKey.getStoreName(), protoKey.getRaw().toByteArray()));
			case DIGEST:
				return new NullableKey(Key.digest(protoKey.getStoreName(), protoKey.getRaw().toByteArray()));
			
			default:
				throw new GkvsException("unknown key type: " + protoKey);
			}
			
		}
		
		return new NullableKey(requestKey);
	}
	
	@Override
	public NullableValue value() {
		
		if (result.hasValue()) {
			return new NullableValue(result.getValue());
		}
		
		return new NullableValue(null);
	}

	@Override
	public String toString() {
		return "RECORD_FOUND [" + requestId() + "]: " + key();
	}

}
