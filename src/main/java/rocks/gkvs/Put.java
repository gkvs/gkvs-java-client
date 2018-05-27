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

import java.util.Map;

import com.google.protobuf.ByteString;

import rocks.gkvs.protos.OperationOptions;
import rocks.gkvs.protos.PutOperation;
import rocks.gkvs.protos.StatusResult;

public final class Put {

	private final GKVSClient instance;

	private final PutOperation.Builder builder = PutOperation.newBuilder();
	
	private Key key;
	private OperationOptions.Builder optionsOrNull;
	
	public Put(GKVSClient instance) {
		this.instance = instance;
	}
	
	public Put setKey(Key key) {
		
		if (key == null) {
			throw new IllegalArgumentException("key is null");
		}
		
		this.key = key;
		return this;
	}
	
	public Put withTimeout(int timeoutMls) {
		if (optionsOrNull == null) {
			optionsOrNull = OperationOptions.newBuilder();
		}
		optionsOrNull.setTimeout(timeoutMls);
		return this;
	}
	
	public Put withPit(long pit) {
		if (optionsOrNull == null) {
			optionsOrNull = OperationOptions.newBuilder();
		}
		optionsOrNull.setPit(pit);
		return this;
	}

	public Put withTtl(int ttlSec) {
		builder.setTtl(ttlSec);
		return this;
	}
	
	public Put compareAndPut(long version) {
		builder.setCompareAndPut(true);
		builder.setVersion(version);
		return this;
	}
	
	public Put put(String column, byte[] value) {
		
		if (column == null) {
			throw new IllegalArgumentException("column is null");
		}

		if (value == null) {
			throw new IllegalArgumentException("value is null");
		}

		rocks.gkvs.protos.Value.Builder valueBuilder = rocks.gkvs.protos.Value.newBuilder();
		valueBuilder.setColumn(column);
		valueBuilder.setRaw(ByteString.copyFrom(value));
		builder.addValue(valueBuilder);
		return this;
	}
	
	public Put putAll(Map<String, byte[]> map) {
		for (Map.Entry<String, byte[]> entry : map.entrySet()) {
			put(entry.getKey(), entry.getValue());
		}
		return this;
	}
	
	public Put put(String column, String value) {
		
		if (column == null) {
			throw new IllegalArgumentException("column is null");
		}

		if (value == null) {
			throw new IllegalArgumentException("value is null");
		}		
		
		rocks.gkvs.protos.Value.Builder valueBuilder = rocks.gkvs.protos.Value.newBuilder();
		valueBuilder.setColumn(column);
		valueBuilder.setRaw(ByteString.copyFrom(value, GKVSConstants.MUTABLE_VALUE_CHARSET));
		builder.addValue(valueBuilder);
		return this;
	}
	
	public Put put(Cell cell) {
		rocks.gkvs.protos.Value.Builder valueBuilder = rocks.gkvs.protos.Value.newBuilder();
		valueBuilder.setColumn(cell.column());
		valueBuilder.setRaw(cell.valueBytes());
		builder.addValue(valueBuilder);
		return this;
	}

	public Put putAll(Cell... cells) {
		for (Cell cell : cells) {
			put(cell);
		}
		return this;
	}
	
	public Put putAll(Iterable<Cell> cells) {
		for (Cell cell : cells) {
			put(cell);
		}
		return this;
	}
	

	public void sync() {
		
		builder.setSequenceNum(instance.nextSequenceNum());
		
		builder.setKey(key.toProto());
		
		if (optionsOrNull != null) {
			builder.setOptions(optionsOrNull);
		}
		
		StatusResult result = instance.getBlockingStub().put(builder.build());
		
		instance.postProcess(result.getStatus());
		
	}
}