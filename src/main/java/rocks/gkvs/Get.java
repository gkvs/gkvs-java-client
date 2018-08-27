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

import com.google.common.util.concurrent.ListenableFuture;

import rocks.gkvs.Transformers.KeyResolver;
import rocks.gkvs.protos.KeyOperation;
import rocks.gkvs.protos.OperationHeader;
import rocks.gkvs.protos.OutputOptions;
import rocks.gkvs.protos.Select;
import rocks.gkvs.protos.ValueResult;

/**
 * 
 * Get
 *
 * Operation
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public final class Get extends One<Record> {

	private final GkvsClient instance;

	private Key key;
	private final OperationHeader.Builder header = OperationHeader.newBuilder();
	private Select.Builder selectOrNull;
	private boolean metadataOnly = false;
	
	public Get(GkvsClient instance) {
		this.instance = instance;
	}
	
	public Get setKey(Key key) {
		this.key = key;
		return this;
	}
	
	public Get withTimeout(int timeoutMls) {
		header.setTimeout(timeoutMls);
		return this;
	}
	
	public Get select(String column) {
		
		if (column == null) {
			throw new IllegalArgumentException("column is null");
		}		
		
		if (selectOrNull == null) {
			selectOrNull = Select.newBuilder();
		}
		selectOrNull.addColumn(column);
		return this;
	}
	
	public Get metadataOnly() {
		this.metadataOnly = true;
		return this;
	}
	
	private KeyOperation buildRequest() {
		
		if (key == null) {
			throw new IllegalArgumentException("key is null");
		}
		
		KeyOperation.Builder builder = KeyOperation.newBuilder();
		
		header.setTag(instance.nextTag());
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
		
		return builder.build();
	}
	
	@Override
	public Record sync() {
		
		try {
			return doSync();
		}
		catch(RuntimeException e) {
			throw new GkvsException("sync fail " + this, e);
		}
	}
	
	private Record doSync() {
		
		ValueResult result = instance.getBlockingStub().get(buildRequest());
		
		return Transformers.toRecord(key, result);
		
	}
	
	@Override
	public GkvsFuture<Record> async() {
		
		ListenableFuture<ValueResult> result = instance.getFutureStub().get(buildRequest());
		
		return new GkvsFuture<Record>(Transformers.toRecord(key, result));
		
	}

	@Override
	public void async(final Observer<Record> recordObserver) {
		
		KeyOperation request = buildRequest();
				
		instance.pushWaitingQueue(request.getHeader().getTag(), key);
		
		final KeyResolver keyResolver = new KeyResolver() {

			@Override
			public Key find(long requestId) {
				return instance.popWaitingQueue(requestId);
			}
			
		};
		
		instance.getAsyncStub().get(request, Transformers.observeRecords(recordObserver, keyResolver));
	
	}
	
	@Override
	public String toString() {
		return "Get " + key;
	}

	
}
