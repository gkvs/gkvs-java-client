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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicReferenceFieldUpdater;

import rocks.gkvs.protos.BatchKeyOperation;
import rocks.gkvs.protos.BatchValueResult;
import rocks.gkvs.protos.KeyOperation;
import rocks.gkvs.protos.OutputOptions;
import rocks.gkvs.protos.RequestOptions;
import rocks.gkvs.protos.Select;

public final class MultiGet implements Resultable {

	private final GKVSClient instance;
	
	private final List<Key> keys = new ArrayList<>();
	
	private final RequestOptions.Builder options = RequestOptions.newBuilder();
	private Select.Builder selectOrNull;
	private boolean metadataOnly = false;
	private boolean includeKey = true;
	
	private final static AtomicReferenceFieldUpdater<MultiGet, BatchValueResult> RESULT_UPDATER
	  = AtomicReferenceFieldUpdater.newUpdater(MultiGet.class, BatchValueResult.class, "result"); 
	  
	private volatile BatchValueResult result;
	
	public MultiGet(GKVSClient instance) {
		this.instance = instance;
	}
	
	public MultiGet setKeys(Key...keys) {
		for (Key key : keys) {
			this.keys.add(key);
		}
		return this;
	}
	
	public MultiGet setKeys(Iterator<Key> keys) {
		while(keys.hasNext()) {
			this.keys.add(keys.next());
		}
		return this;
	}
	
	public MultiGet setKeys(Iterable<Key> keys) {
		setKeys(keys.iterator());
		return this;
	}
	
	public MultiGet addKey(Key key) {
		this.keys.add(key);
		return this;
	}
	
	public MultiGet includeKey(boolean flag) {
		this.includeKey = flag;
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
	
	public Iterable<Record> sync() {
		
		BatchKeyOperation.Builder builder = BatchKeyOperation.newBuilder();
		
		for (Key key : keys) {
			builder.addOperation(buildKeyOperation(key));
		}
		
		final BatchValueResult result = instance.getBlockingStub().multiGet(builder.build());
		RESULT_UPDATER.set(this, result);
		
		return new Iterable<Record>() {

			@Override
			public Iterator<Record> iterator() {
				return Transformers.toRecords(result.getResultList().iterator());
			}
			
		};
		
	}
	
	@Override
	public String result() {
		return result != null ? result.toString() : null;
	}
	

	
}
