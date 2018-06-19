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

import io.grpc.stub.StreamObserver;
import rocks.gkvs.Transformers.KeyResolver;
import rocks.gkvs.protos.PutOperation;
import rocks.gkvs.protos.RequestOptions;

/**
 * 
 * PutAll
 * 
 * Operation
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public final class PutAll {

	private final GkvsClient instance;
	
	private int timeoutMls;
	private long pit;
	private int ttlSec;
	
	public PutAll(GkvsClient instance) {
		this.instance = instance;
	}
	
	public PutAll withTimeout(int timeoutMls) {
		this.timeoutMls = timeoutMls;
		return this;
	}
	
	public PutAll withPit(long pit) {
		this.pit = pit;
		return this;
	}
	
	public PutAll withTtl(int ttlSec) {
		this.ttlSec = ttlSec;
		return this;
	}
	
	private PutOperation.Builder buildRequest(KeyValue keyValue) {
		
		if (keyValue == null) {
			throw new IllegalArgumentException("keyValue is null");
		}
		
		PutOperation.Builder builder = PutOperation.newBuilder();
		
		RequestOptions.Builder options = RequestOptions.newBuilder();
		
		options.setRequestId(instance.nextRequestId());
		options.setTimeout(timeoutMls);
		options.setPit(pit);
		
		builder.setOptions(options);
		
		builder.setKey(keyValue.key().toProto());
		builder.setTtl(ttlSec);
		
		for (Value value : keyValue.values()) {
			builder.addValue(value.toProto());
		}

		builder.setTtl(ttlSec);

		return builder;
	}
	
	public Iterable<Status> sync(Iterable<KeyValue> keyValues) {
		
		BlockingCollector<Status> collector = new BlockingCollector<Status>();
		
		Observer<KeyValue> keyValueChannel = async(collector);
		
		for (KeyValue keyValue : keyValues) {
			keyValueChannel.onNext(keyValue);
		}
		
		keyValueChannel.onCompleted();
		
		return collector.awaitUnchecked();
	}
	
	public Observer<KeyValue> async(Observer<Status> statusObserver) {
		
		final KeyResolver keyResolver = new KeyResolver() {

			@Override
			public Key find(long requestId) {
				return instance.popWaitingQueue(requestId);
			}
			
		};
		
		final StreamObserver<PutOperation> streamIn = instance.getAsyncStub().putAll(Transformers.observeStatuses(statusObserver, keyResolver));
		
		return new Observer<KeyValue>() {

			@Override
			public void onNext(KeyValue keyValue) {
				PutOperation op = buildRequest(keyValue).build();
				instance.pushWaitingQueue(op.getOptions().getRequestId(), keyValue.key());
				streamIn.onNext(op);
			}

			@Override
			public void onError(Throwable t) {
				streamIn.onError(t);
			}

			@Override
			public void onCompleted() {
				streamIn.onCompleted();
			}
			
		};
		
	}

	@Override
	public String toString() {
		return "PutAll";
	}
	
}
