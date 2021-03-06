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
import rocks.gkvs.protos.KeyOperation;
import rocks.gkvs.protos.OperationHeader;
import rocks.gkvs.protos.OutputOptions;
import rocks.gkvs.protos.Select;

/**
 * 
 * GetAll
 * 
 * Operation
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public final class GetAll extends BiStream<Key, Record> {

	private final GkvsClient instance;
	
	private int timeoutMls;
	
	private Select.Builder selectOrNull;
	private boolean metadataOnly = false;
		
	public GetAll(GkvsClient instance) {
		this.instance = instance;
	}
	
	public GetAll select(String column) {
		
		if (column == null) {
			throw new IllegalArgumentException("column is null");
		}		
		
		if (selectOrNull == null) {
			selectOrNull = Select.newBuilder();
		}
		selectOrNull.addColumn(column);
		return this;
	}
	
	public GetAll withTimeout(int timeoutMls) {
		this.timeoutMls = timeoutMls;
		return this;
	}
	
	public GetAll metadataOnly() {
		this.metadataOnly = true;
		return this;
	}

	private KeyOperation.Builder buildRequest(Key key) {
		
		KeyOperation.Builder builder = KeyOperation.newBuilder();
		
		OperationHeader.Builder header = OperationHeader.newBuilder();
		header.setTag(instance.nextTag());
		header.setTimeout(timeoutMls);
		builder.setHeader(header);
		
		builder.setKey(key.toProto());
		
		if (metadataOnly) {
			builder.setOutput(OutputOptions.METADATA);
		}
		else {
			builder.setOutput(OutputOptions.VALUE);
		}
		
		if (selectOrNull != null) {
			builder.setSelect(selectOrNull);
		}
		
		return builder;
		
	}
	
	@Override
	public Iterable<Record> sync(Iterable<Key> keys) {
		
		BlockingCollector<Record> collector = new BlockingCollector<Record>();
		
		Observer<Key> keyChannel = async(collector);
		
		for (Key key : keys) {
			keyChannel.onNext(key);
		}
		
		keyChannel.onCompleted();
		
		return collector.awaitUnchecked();
	}
	
	@Override
	public Observer<Key> async(final Observer<Record> recordObserver) {
		
		final KeyResolver keyResolver = new KeyResolver() {

			@Override
			public Key find(long requestId) {
				return instance.popWaitingQueue(requestId);
			}
			
		};
		
		final StreamObserver<KeyOperation> streamOut = instance.getAsyncStub().getAll(Transformers.observeRecords(recordObserver, keyResolver));
		
		return new Observer<Key>() {

			@Override
			public void onNext(Key key) {
				KeyOperation op = buildRequest(key).build();
				instance.pushWaitingQueue(op.getHeader().getTag(), key);
				streamOut.onNext(op);
			}

			@Override
			public void onError(Throwable t) {
				streamOut.onError(t);
			}

			@Override
			public void onCompleted() {
				streamOut.onCompleted();
			}
			
		};
	}

	@Override
	public String toString() {
		return "GetAll";
	}
	
}
