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

import com.google.common.util.concurrent.ListenableFuture;

import rocks.gkvs.Transformers.KeyResolver;
import rocks.gkvs.protos.OperationHeader;
import rocks.gkvs.protos.PutOperation;
import rocks.gkvs.protos.StatusResult;
import rocks.gkvs.value.Value;

/**
 * 
 * Put
 * 
 * Operation
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public final class Put {

	private final GkvsClient instance;

	private final PutOperation.Builder builder = PutOperation.newBuilder();
	
	private Key key;
	private Value value;
	private final OperationHeader.Builder header = OperationHeader.newBuilder();
		
	public Put(GkvsClient instance) {
		this.instance = instance;
	}
	
	public Put withTimeout(int timeoutMls) {
		header.setTimeout(timeoutMls);
		return this;
	}

	public Put withTtl(int ttlSec) {
		builder.setTtl(ttlSec);
		return this;
	}
	
	public Put putIfAbsent(KeyValue keyValue) {
		return compareAndPut(keyValue, null);
	}
	
	public Put compareAndPut(KeyValue keyValue, @Nullable int[] version) {
		return compareAndPut(keyValue.key(), keyValue.value(), version);
	}
	
	public Put putIfAbsent(Key key, Value value) {
		return compareAndPut(key, value, null);
	}
	
	public Put compareAndPut(Key key, Value value, @Nullable int[] version) {
		this.key = key;
		this.value = value;
		builder.setCompareAndPut(true);

		// if version not exists, then it is PutIfAbsent
		if (version != null) {
			int size = version.length;
			for (int i = 0; i != size; ++i) {
				builder.addVersion(version[i]);
			}
		}
		
		return this;
	}
	
	public Put put(KeyValue keyValue) {
		this.key = keyValue.key();
		this.value = keyValue.value();
		return this;
	}
		
	public Put put(Key key, Value value) {
		this.key = key;
		this.value = value;
		return this;
	}
	
	private PutOperation buildRequest() {
		
		if (key == null) {
			throw new IllegalArgumentException("key is null");
		}
		
		if (value == null) {
			throw new IllegalArgumentException("value is null");
		}
		
		header.setTag(instance.nextTag());
		builder.setHeader(header);
		
		builder.setKey(key.toProto());
		builder.setValue(Transformers.toProto(value));
		
		return builder.build();
	}

	public Status sync() {
		
		try {
			return doSync();
		}
		catch(RuntimeException e) {
			throw new GkvsException("sync fail " + this, e);
		}
		
	}
	
	private Status doSync() {
		
		final StatusResult result = instance.getBlockingStub().put(buildRequest());
		
		return Transformers.toStatus(key, result);
		
	}
	
	public GkvsFuture<Status> async() {
		
		ListenableFuture<StatusResult> result = instance.getFutureStub().put(buildRequest());
		
		return new GkvsFuture<Status>(Transformers.toStatus(key, result));
		
	}
	
	public void async(final Observer<Status> statusObserver) {
		
		PutOperation request = buildRequest();
		
		instance.pushWaitingQueue(request.getHeader().getTag(), key);
		
		final KeyResolver keyResolver = new KeyResolver() {

			@Override
			public Key find(long requestId) {
				return instance.popWaitingQueue(requestId);
			}
			
		};
		
		instance.getAsyncStub().put(request, Transformers.observeStatuses(statusObserver, keyResolver));
	
	}

	@Override
	public String toString() {
		return "Put " + key;
	}

	
}
