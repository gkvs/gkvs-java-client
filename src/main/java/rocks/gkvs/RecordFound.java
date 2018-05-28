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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import rocks.gkvs.protos.Value;
import rocks.gkvs.protos.ValueResult;

public final class RecordFound implements Record {

	private final ValueResult result;
	
	protected RecordFound(ValueResult result) {
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
				return new NullableKey(Key.raw(protoKey.getTableName(), protoKey.getRaw().toByteArray()));
			case DIGEST:
				return new NullableKey(Key.digest(protoKey.getTableName(), protoKey.getRaw().toByteArray()));
			
			default:
				throw new GKVSException("unknown key type: " + protoKey);
			}
			
		}
		
		return new NullableKey(null);
	}
	
	@Override
	public NullableValue value() {
		
		if (result.getValueCount() == 0) {
			return new NullableValue(null);
		}
		
		return new NullableValue(new rocks.gkvs.Value(result.getValue(0)));
	}
	
	@Override
	public List<rocks.gkvs.Value> valueList() {
		
		List<rocks.gkvs.Value> list = new ArrayList<>(result.getValueCount());
		
		for (Value value : result.getValueList()) {
			list.add(new rocks.gkvs.Value(value));
		}
		
		return list;
		
	}
	
	@Override
	public Map<String, rocks.gkvs.Value> valueMap() {
		
		Map<String, rocks.gkvs.Value> map = new HashMap<String, rocks.gkvs.Value>();
		
		for (Value value : result.getValueList()) {
			map.put(value.getColumn(), new rocks.gkvs.Value(value));
		}
		
		return map;
		
	}

	@Override
	public String toString() {
		return "RECORD_FOUND [" + requestId() + "]: " + key();
	}

}
