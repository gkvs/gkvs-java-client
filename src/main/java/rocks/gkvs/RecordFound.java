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

import javax.annotation.Nullable;

import com.google.protobuf.ByteString;

import rocks.gkvs.protos.Value;
import rocks.gkvs.protos.ValueResult;

public class RecordFound implements Record {

	private final ValueResult result;
	
	protected RecordFound(ValueResult result) {
		this.result = result;
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
	public int leftTtl() {
		return result.getMetadata().getTtl();
	}
	
	@Override
	public @Nullable Key key() {
		
		if (result.hasKey()) {
			
			rocks.gkvs.protos.Key protoKey = result.getKey();
			
			switch(protoKey.getRecordKeyCase()) {
			
			case RAW:
				return Key.raw(protoKey.getTableName(), protoKey.getRaw().toByteArray());
			case DIGEST:
				return Key.digest(protoKey.getTableName(), protoKey.getRaw().toByteArray());
			
			default:
				throw new GKVSException("unknown key type: " + protoKey);
			}
			
		}
		
		return null;
	}
	
	@Override
	public @Nullable byte[] value() {
		
		if (result.getValueCount() > 1) {
			throw new GKVSException("expected a single value in result");
		}
		
		if (result.getValueCount() == 0) {
			return null;
		}
		
		return getValuePayload(result.getValue(0)).toByteArray();
	}
	
	@Override
	public @Nullable String valueAsString() {
		
		if (result.getValueCount() > 1) {
			throw new GKVSException("expected a single value in result");
		}
		
		if (result.getValueCount() == 0) {
			return null;
		}
		
		return getValuePayload(result.getValue(0)).toString(GKVSConstants.MUTABLE_VALUE_CHARSET);
		
	}
	
	@Override
	public @Nullable List<rocks.gkvs.Value> valueList() {
		
		List<rocks.gkvs.Value> list = new ArrayList<>(result.getValueCount());
		
		for (Value value : result.getValueList()) {
			list.add(new rocks.gkvs.Value(value.getColumn(),  getValuePayload(value), 0));
		}
		
		return list;
		
	}
	
	
	@Override
	public @Nullable  Map<String, byte[]> valueMap() {
		
		Map<String, byte[]> map = new HashMap<String, byte[]>();
		
		for (Value value : result.getValueList()) {
			map.put(value.getColumn(), getValuePayload(value).toByteArray());
		}
		
		return map;
		
	}
	
	private @Nullable ByteString getValuePayload(Value value) {

		switch (value.getValueCase()) {
		case RAW:
			return value.getRaw();
		case DIGEST:
			return value.getDigest();
		default:
			return ByteString.EMPTY;
		}
	}
}
