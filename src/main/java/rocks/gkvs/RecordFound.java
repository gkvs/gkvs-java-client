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

import rocks.gkvs.protos.Metadata;
import rocks.gkvs.protos.ValueResult;
import rocks.gkvs.value.Nil;
import rocks.gkvs.value.Value;

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
	public long tag() {
		return result.getHeader().getTag();
	}

	@Override
	public boolean exists() {
		return true;
	}
	
	@Override
	public int[] version() {
		Metadata metadata = result.getMetadata();
		int size = metadata.getVersionCount();
		int[] result = new int[size];
		for (int i = 0; i != size; ++i) {
			result[i] = metadata.getVersion(i);
		}
		return result;
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
				return new NullableKey(Key.raw(protoKey.getTableName(), protoKey.getRaw().toByteArray()));
			case DIGEST:
				return new NullableKey(Key.digest(protoKey.getTableName(), protoKey.getRaw().toByteArray()));
			
			default:
				throw new GkvsException("unknown key type: " + protoKey.getRecordKeyCase());
			}
			
		}
		
		return new NullableKey(requestKey);
	}
	
	@Override
	public boolean hasValue() {
		return result.hasValue();
	}
	
	@Override
	public Value value() {
		return result.hasValue() ? Transformers.fromProto(result.getValue()) : Nil.get();
	}

	@Override
	public @Nullable byte[] rawValue() {
		
		if (result.hasValue()) {
			rocks.gkvs.protos.Value proto = result.getValue();
			return proto.getRaw().toByteArray();
		}
		
		return null;
	}

	@Override
	public String toString() {
		return "RECORD_FOUND [" + tag() + "]: " + key() + "=" + value();
	}

}
