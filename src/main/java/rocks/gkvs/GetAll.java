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
import rocks.gkvs.protos.KeyOperation;
import rocks.gkvs.protos.OutputOptions;
import rocks.gkvs.protos.RequestOptions;
import rocks.gkvs.protos.Select;

public final class GetAll {

	private final GKVSClient instance;
	
	private final RequestOptions.Builder options = RequestOptions.newBuilder();
	private Select.Builder selectOrNull;
	private boolean metadataOnly = false;
	private boolean includeKey = true;
	
	public GetAll(GKVSClient instance) {
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
	
	public GetAll includeKey(boolean flag) {
		this.includeKey = flag;
		return this;
	}
	
	public GetAll withTimeout(int timeoutMls) {
		options.setTimeout(timeoutMls);
		return this;
	}
	
	public GetAll withPit(long pit) {
		options.setPit(pit);
		return this;
	}
	
	public GetAll metadataOnly() {
		this.metadataOnly = true;
		return this;
	}

	private KeyOperation.Builder buildKeyOperation(Key key) {
		
		KeyOperation.Builder builder = KeyOperation.newBuilder();
		
		options.setRequestId(instance.nextRequestId());
		builder.setOptions(options);
		
		builder.setKey(key.toProto());
		
		if (metadataOnly) {
			builder.setOutput(OutputOptions.METADATA_ONLY);
		}
		else if (includeKey) {
			builder.setOutput(OutputOptions.KEY_VALUE_RAW);
		}
		else {
			builder.setOutput(OutputOptions.VALUE_RAW);
		}
		
		if (selectOrNull != null) {
			builder.setSelect(selectOrNull);
		}
		
		return builder;
		
	}
	
	public KeyObserver async(final RecordObserver recordObserver) {
		
		final StreamObserver<KeyOperation> streamOut = instance.getAsyncStub().getAll(Transformers.observe(recordObserver));
		
		return new KeyObserver() {

			@Override
			public void onNext(Key key) {
				KeyOperation op = buildKeyOperation(key).build();
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
	
}
