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
import rocks.gkvs.protos.OperationOptions;
import rocks.gkvs.protos.OutputOptions;
import rocks.gkvs.protos.Select;
import rocks.gkvs.protos.ValueResult;

public final class Get implements Resultable {

	private final GKVSClient instance;

	private Key key;
	private OperationOptions.Builder optionsOrNull;
	private Select.Builder selectOrNull;
	
	private final static AtomicReferenceFieldUpdater<Get, ValueResult> RESULT_UPDATER
	  = AtomicReferenceFieldUpdater.newUpdater(Get.class, ValueResult.class, "result"); 
	  
	private volatile ValueResult result;
	
	public Get(GKVSClient instance) {
		this.instance = instance;
	}
	
	public Get setKey(Key key) {
		
		if (key == null) {
			throw new IllegalArgumentException("key is null");
		}
		
		this.key = key;
		return this;
	}
	
	public Get withTimeout(int timeoutMls) {
		if (optionsOrNull == null) {
			optionsOrNull = OperationOptions.newBuilder();
		}
		optionsOrNull.setTimeout(timeoutMls);
		return this;
	}
	
	public Get withPit(long pit) {
		if (optionsOrNull == null) {
			optionsOrNull = OperationOptions.newBuilder();
		}
		optionsOrNull.setPit(pit);
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
	
	public Record sync() {
		
		KeyOperation.Builder builder = KeyOperation.newBuilder();
		
		builder.setSequenceNum(instance.nextSequenceNum());
		
		builder.setKey(key.toProto());
		builder.setOutput(OutputOptions.VALUE_RAW);
		
		if (optionsOrNull != null) {
			builder.setOptions(optionsOrNull);
		}
		
		if (selectOrNull != null) {
			builder.setSelect(selectOrNull);
		}
		
		ValueResult result = instance.getBlockingStub().get(builder.build());
		RESULT_UPDATER.set(this, result);
		
		instance.postProcess(result.getStatus(), this);
		
		if (result.hasMetadata()) {
			return new RecordFound(result);
		}
		else {
			// record not found
			return RecordNotFound.RECORD_NOT_FOUND;
		}
		
	}
	
	//public Future<ValueSet> async() {
	//	return null;
	//}
	
	@Override
	public String result() {
		return result.toString();
	}
	
}
