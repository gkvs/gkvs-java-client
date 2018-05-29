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

import com.google.common.util.concurrent.ListenableFuture;

import rocks.gkvs.protos.PutOperation;
import rocks.gkvs.protos.RequestOptions;
import rocks.gkvs.protos.StatusResult;

public final class Put {

	private final GKVSClient instance;

	private final PutOperation.Builder builder = PutOperation.newBuilder();
	
	private Key key;
	private final RequestOptions.Builder options = RequestOptions.newBuilder();
		
	public Put(GKVSClient instance) {
		this.instance = instance;
	}
	
	public Put setKey(Key key) {
		this.key = key;
		return this;
	}
	
	public Put withTimeout(int timeoutMls) {
		options.setTimeout(timeoutMls);
		return this;
	}
	
	public Put withPit(long pit) {
		options.setPit(pit);
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
	
	public Put put(String column, String value) {
		builder.addValue(Value.of(column, value).toProto());
		return this;
	}

	public Put put(String column, byte[] value) {
		builder.addValue(Value.of(column, value).toProto());
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

	private PutOperation buildRequest() {
		
		if (key == null) {
			throw new IllegalArgumentException("key is null");
		}
		
		options.setRequestId(instance.nextRequestId());
		builder.setOptions(options);
		
		builder.setKey(key.toProto());
		
		return builder.build();
	}

	public Status sync() {
		
		final StatusResult result = instance.getBlockingStub().put(buildRequest());
		
		return Transformers.toStatus(key, result);
	}
	
	public StatusFuture async() {
		
		ListenableFuture<StatusResult> result = instance.getFutureStub().put(buildRequest());
		
		return new StatusFuture(Transformers.toStatus(key, result));
		
	}


	
}
