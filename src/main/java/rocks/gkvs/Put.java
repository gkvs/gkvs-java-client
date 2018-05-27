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
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import rocks.gkvs.protos.PutOperation;
import rocks.gkvs.protos.RequestOptions;
import rocks.gkvs.protos.StatusCode;
import rocks.gkvs.protos.StatusResult;

public final class Put implements Resultable {

	private final GKVSClient instance;

	private final PutOperation.Builder builder = PutOperation.newBuilder();
	
	private Key key;
	private RequestOptions.Builder optionsOrNull;
	
	private final static AtomicReferenceFieldUpdater<Put, StatusResult> RESULT_UPDATER
	  = AtomicReferenceFieldUpdater.newUpdater(Put.class, StatusResult.class, "result"); 
	  
	private volatile StatusResult result;
	
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
			optionsOrNull = RequestOptions.newBuilder();
		}
		optionsOrNull.setTimeout(timeoutMls);
		return this;
	}
	
	public Put withPit(long pit) {
		if (optionsOrNull == null) {
			optionsOrNull = RequestOptions.newBuilder();
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
		
	public Put put(Value value) {
		builder.addValue(value.toProto());
		return this;
	}

	public Put putAll(Value... cells) {
		for (Value cell : cells) {
			put(cell);
		}
		return this;
	}
	
	public Put putAll(Iterable<Value> cells) {
		for (Value cell : cells) {
			put(cell);
		}
		return this;
	}
	
	public Put putAll(Map<String, byte[]> map) {
		for (Map.Entry<String, byte[]> entry : map.entrySet()) {
			put(Value.of(entry.getKey(), entry.getValue()));
		}
		return this;
	}
	

	/**
	 * @return true if updated, used only for CompareAndPut operation
	 */
	
	public boolean sync() {
		
		builder.setSequenceNum(instance.nextSequenceNum());
		
		builder.setKey(key.toProto());
		
		if (optionsOrNull != null) {
			builder.setOptions(optionsOrNull);
		}
		
		RESULT_UPDATER.set(this, instance.getBlockingStub().put(builder.build()));
		
		instance.postProcess(result.getStatus(), this);
		
		if (result.getStatus().getCode() == StatusCode.SUCCESS) {
			return true;
		}
		else if (result.getStatus().getCode() == StatusCode.SUCCESS_NOT_UPDATED) {
			return false;
		}
		else {
			throw new GKVSException("unknown status code: " + result.getStatus());
		}
		
	}

	@Override
	public String result() {
		return result.toString();
	}
	
}
