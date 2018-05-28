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
import rocks.gkvs.protos.RequestOptions;
import rocks.gkvs.protos.Select;
import rocks.gkvs.protos.StatusCode;
import rocks.gkvs.protos.StatusResult;

public final class Remove implements Resultable {

	private final GKVSClient instance;
	
	private Key key;
	private RequestOptions.Builder optionsOrNull;
	private Select.Builder selectOrNull;
	
	private final static AtomicReferenceFieldUpdater<Remove, StatusResult> RESULT_UPDATER
	  = AtomicReferenceFieldUpdater.newUpdater(Remove.class, StatusResult.class, "result"); 
	  
	private volatile StatusResult result;
	
	public Remove(GKVSClient instance) {
		this.instance = instance;
	}
	
	public Remove setKey(Key key) {
		
		if (key == null) {
			throw new IllegalArgumentException("key is null");
		}
		
		this.key = key;
		return this;
	}
	
	public Remove withTimeout(int timeoutMls) {
		if (optionsOrNull == null) {
			optionsOrNull = RequestOptions.newBuilder();
		}
		optionsOrNull.setTimeout(timeoutMls);
		return this;
	}
	
	public Remove withPit(long pit) {
		if (optionsOrNull == null) {
			optionsOrNull = RequestOptions.newBuilder();
		}
		optionsOrNull.setPit(pit);
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

	public boolean sync() {
		
		KeyOperation.Builder builder = KeyOperation.newBuilder();
		
		builder.setSequenceNum(instance.nextSequenceNum());
		
		builder.setKey(key.toProto());
		
		if (optionsOrNull != null) {
			builder.setOptions(optionsOrNull);
		}
		
		if (selectOrNull != null) {
			builder.setSelect(selectOrNull);
		}
		
		RESULT_UPDATER.set(this, instance.getBlockingStub().remove(builder.build()));
		
		instance.postProcess(result.getStatus());
		
		if (result.getStatus().getCode() == StatusCode.SUCCESS) {
			return true;
		}
		else if (result.getStatus().getCode() == StatusCode.SUCCESS_NOT_UPDATED) {
			return false;
		}
		else {
			throw new GKVSException("unknown status code: " + result.getStatus());
		}
		
	}
	
	@Override
	public String result() {
		return result != null ? result.toString() : null;
	}
	
}
