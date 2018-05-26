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

import rocks.gkvs.protos.KeyOperation;
import rocks.gkvs.protos.OperationOptions;
import rocks.gkvs.protos.OutputOptions;
import rocks.gkvs.protos.Select;
import rocks.gkvs.protos.ValueResult;

public class Get {

	private final GKVSClient instance;

	private Key key;
	private int timeoutMls;
	private long pit;
	
	private Select.Builder selectOrNull;
	
	public Get(GKVSClient instance) {
		this.instance = instance;
	}
	
	public Get setKey(Key key) {
		this.key = key;
		return this;
	}
	
	public Get withTimeout(int timeoutMls) {
		this.timeoutMls = timeoutMls;
		return this;
	}
	
	public Get withPit(long pit) {
		this.pit = pit;
		return this;
	}
	
	public Get select(String columnName) {
		if (selectOrNull == null) {
			selectOrNull = Select.newBuilder();
		}
		selectOrNull.addColumn(columnName);
		return this;
	}
	
	
	public Record sync() {
		
		KeyOperation.Builder builder = KeyOperation.newBuilder();
		
		builder.setSequenceNum(instance.nextSequenceNum());
		
		builder.setKey(key.toProto());
		builder.setOutput(OutputOptions.VALUE_RAW);
		
		if (timeoutMls > 0 || pit > 0) {
			OperationOptions.Builder options = OperationOptions.newBuilder();
			
			if (timeoutMls > 0) {
				options.setTimeout(timeoutMls);
			}
			
			if (pit > 0) {
				options.setPit(pit);
			}
			
			builder.setOptions(options);
		}
		
		if (selectOrNull != null) {
			builder.setSelect(selectOrNull);
		}
		
		ValueResult result = instance.getBlockingStub().get(builder.build());
		
		instance.postProcess(result.getStatus());
		
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
	
}
