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

import io.grpc.stub.StreamObserver;
import rocks.gkvs.Transformers.KeyResolver;
import rocks.gkvs.protos.KeyOperation;
import rocks.gkvs.protos.RequestOptions;
import rocks.gkvs.protos.Select;

public final class RemoveAll {

	private final GKVSClient instance;
	
	private Select.Builder selectOrNull;
	private int timeoutMls;
	private long pit;
	
	public RemoveAll(GKVSClient instance) {
		this.instance = instance;
	}
	
	public RemoveAll select(String column) {
		
		if (column == null) {
			throw new IllegalArgumentException("column is null");
		}		
		
		if (selectOrNull == null) {
			selectOrNull = Select.newBuilder();
		}
		selectOrNull.addColumn(column);
		return this;
	}
	
	public RemoveAll withTimeout(int timeoutMls) {
		this.timeoutMls = timeoutMls;
		return this;
	}
	
	public RemoveAll withPit(long pit) {
		this.pit = pit;
		return this;
	}

	private KeyOperation buildRequest(Key key) {
		
		if (key == null) {
			throw new IllegalArgumentException("key is null");
		}
		
		KeyOperation.Builder builder = KeyOperation.newBuilder();
		
		RequestOptions.Builder options = RequestOptions.newBuilder();
		options.setRequestId(instance.nextRequestId());
		options.setTimeout(timeoutMls);
		options.setPit(pit);
		
		builder.setOptions(options);
		
		builder.setKey(key.toProto());
		
		if (selectOrNull != null) {
			builder.setSelect(selectOrNull);
		}
		
		return builder.build();
	}
	
	public Iterable<Status> sync(Iterable<Key> keys) {
		
		BlockingCollector<Status> collector = new BlockingCollector<Status>();
		
		GObserver<Key> keyChannel = async(collector);
		
		for (Key key : keys) {
			keyChannel.onNext(key);
		}
		
		keyChannel.onCompleted();
		
		return collector.awaitUnchecked();
	}
	
	public GObserver<Key> async(GObserver<Status> statusObserver) {
		
		final KeyResolver keyResolver = new KeyResolver() {

			@Override
			public Key find(long requestId) {
				return instance.popWaitingQueue(requestId);
			}
			
		};
		
		final StreamObserver<KeyOperation> streamIn = instance.getAsyncStub().removeAll(Transformers.observeStatuses(statusObserver, keyResolver));
		
		return new GObserver<Key>() {

			@Override
			public void onNext(Key key) {
				KeyOperation op = buildRequest(key);
				instance.pushWaitingQueue(op.getOptions().getRequestId(), key);
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
	
}
