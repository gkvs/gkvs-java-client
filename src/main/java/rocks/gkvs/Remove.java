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
import rocks.gkvs.protos.Select;
import rocks.gkvs.protos.StatusResult;

/**
 * 
 * Remove
 * 
 * Operation
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public final class Remove extends One<Status> {

	private final GkvsClient instance;
	
	private Key key;
	private final OperationHeader.Builder header = OperationHeader.newBuilder();
	private Select.Builder selectOrNull;
	
	public Remove(GkvsClient instance) {
		this.instance = instance;
	}
	
	public Remove setKey(Key key) {
		this.key = key;
		return this;
	}
	
	public Remove withTimeout(int timeoutMls) {
		header.setTimeout(timeoutMls);
		return this;
	}
	
	public Remove select(String column) {
		
		if (column == null) {
			throw new IllegalArgumentException("column is null");
		}		
		
		if (selectOrNull == null) {
			selectOrNull = Select.newBuilder();
		}
		selectOrNull.addColumn(column);
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
		
		if (selectOrNull != null) {
			builder.setSelect(selectOrNull);
		}
		
		return builder.build();
	}
	
	@Override
	public Status sync() {
		
		try {
			return doSync();
		}
		catch(RuntimeException e) {
			throw new GkvsException("sync fail " + this, e);
		}
		
	}
	
	private Status doSync() {
		
		StatusResult result = instance.getBlockingStub().remove(buildRequest());
		
		return Transformers.toStatus(key, result);
		
	}
	
	@Override
	public GkvsFuture<Status> async() {
		
		ListenableFuture<StatusResult> result = instance.getFutureStub().remove(buildRequest());
		
		return new GkvsFuture<Status>(Transformers.toStatus(key, result));
		
	}
	
	@Override
	public void async(final Observer<Status> statusObserver) {
		
		KeyOperation request = buildRequest();
		
		instance.pushWaitingQueue(request.getHeader().getTag(), key);
		
		final KeyResolver keyResolver = new KeyResolver() {

			@Override
			public Key find(long requestId) {
				return instance.popWaitingQueue(requestId);
			}
			
		};
		
		instance.getAsyncStub().remove(request, Transformers.observeStatuses(statusObserver, keyResolver));
	
	}
	
	@Override
	public String toString() {
		return "Remove " + key;
	}
	
}
