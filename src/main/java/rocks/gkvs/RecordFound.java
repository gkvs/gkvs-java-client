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
	public int ttl() {
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
		
		return getPayload(result.getValue(0));
	}
	
	@Override
	public @Nullable List<Cell> valueList() {
		
		List<Cell> list = new ArrayList<Cell>(result.getValueCount());
		
		for (Value value : result.getValueList()) {
			list.add(new Cell(value.getColumn(),  getPayload(value)));
		}
		
		return list;
		
	}
	
	
	@Override
	public @Nullable  Map<String, byte[]> valueMap() {
		
		Map<String, byte[]> map = new HashMap<String, byte[]>();
		
		for (Value value : result.getValueList()) {
			map.put(value.getColumn(),  getPayload(value));
		}
		
		return map;
		
	}
	
	
	private byte[] getPayload(Value value) {

		switch (value.getValueCase()) {
		case RAW:
			return value.getRaw().toByteArray();
		case DIGEST:
			return value.getDigest().toByteArray();
		default:
			throw new GKVSException("unknown value type: " + value);
		}
	}
}
