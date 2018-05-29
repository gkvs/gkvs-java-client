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

import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import rocks.gkvs.protos.KeyOperation;
import rocks.gkvs.protos.OutputOptions;
import rocks.gkvs.protos.RequestOptions;
import rocks.gkvs.protos.Select;
import rocks.gkvs.protos.ValueResult;

public final class Get implements Resultable {

	private final GKVSClient instance;

	private Key key;
	private final RequestOptions.Builder options = RequestOptions.newBuilder();
	private Select.Builder selectOrNull;
	private boolean metadataOnly = false;
	
	private final static AtomicReferenceFieldUpdater<Get, ValueResult> RESULT_UPDATER
	  = AtomicReferenceFieldUpdater.newUpdater(Get.class, ValueResult.class, "result"); 
	  
	private volatile ValueResult result;
	
	public Get(GKVSClient instance) {
		this.instance = instance;
	}
	
	public Get setKey(Key key) {
		this.key = key;
		return this;
	}
	
	public Get withTimeout(int timeoutMls) {
		options.setTimeout(timeoutMls);
		return this;
	}
	
	public Get withPit(long pit) {
		options.setPit(pit);
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
	
	public Record sync() {
		
		if (key == null) {
			throw new IllegalArgumentException("key is null");
		}
		
		KeyOperation.Builder builder = KeyOperation.newBuilder();
		
		options.setRequestId(instance.nextRequestId());
		builder.setOptions(options);
		
		builder.setKey(key.toProto());
		
		if (metadataOnly) {
			builder.setOutput(OutputOptions.METADATA_ONLY);
		}
		else {
			builder.setOutput(OutputOptions.VALUE_RAW);
		}
				
		if (selectOrNull != null) {
			builder.setSelect(selectOrNull);
		}
		
		ValueResult result = instance.getBlockingStub().get(builder.build());
		RESULT_UPDATER.set(this, result);
		
		return Transformers.toRecord(key, result);
	}
	
	//public Future<ValueSet> async() {
	//	return null;
	//}
	
	@Override
	public String result() {
		return result != null ? result.toString() : null;
	}
	
}
