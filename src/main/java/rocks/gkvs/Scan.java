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

import java.util.Iterator;

import rocks.gkvs.Transformers.NullKeyResolver;
import rocks.gkvs.protos.OperationHeader;
import rocks.gkvs.protos.ScanOperation;
import rocks.gkvs.protos.Select;
import rocks.gkvs.protos.ValueResult;

/**
 * 
 * Scan
 * 
 * Operation
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public final class Scan extends IncomingStream<Record> {

	private final GkvsClient instance;

	private String viewName;
	private final OperationHeader.Builder header = OperationHeader.newBuilder();
	private Select.Builder selectOrNull;
	
	private boolean includeKey = true;
	private boolean includeValue = false;
	
	public Scan(GkvsClient instance) {
		this.instance = instance;
	}
	
	public Scan view(String viewName) {
		this.viewName = viewName;
		return this;
	}
	
	public Scan includeKey(boolean outputKey) {
		this.includeKey = outputKey; 
		return this;
	}

	public Scan includeValue(boolean outputValue) {
		this.includeValue = outputValue; 
		return this;
	}

	public Scan select(String column) {
		
		if (column == null) {
			throw new IllegalArgumentException("column is null");
		}		
		
		if (selectOrNull == null) {
			selectOrNull = Select.newBuilder();
		}
		selectOrNull.addColumn(column);
		return this;
	}
	
	private ScanOperation buildRequest() {
		
		ScanOperation.Builder builder = ScanOperation.newBuilder();
		
		if (viewName == null) {
			throw new IllegalArgumentException("store name is null");
		}
		
		header.setTag(instance.nextTag());
		builder.setHeader(header);
		
		builder.setViewName(viewName);
		builder.setOutput(ProtocolUtils.getOutput(includeKey, includeValue));
		
		if (selectOrNull != null) {
			builder.setSelect(selectOrNull);
		}
		
		return builder.build();
		
	}
	
	@Override
	public Iterator<Record> sync() {
		
		Iterator<ValueResult> results = instance.getBlockingStub().scan(buildRequest());
		return Transformers.toRecords(results);
		
	}
	
	@Override
	public void async(Observer<Record> recordObserver) {
		
		instance.getAsyncStub().scan(buildRequest(), Transformers.observeRecords(recordObserver, NullKeyResolver.INS));
		
	}

	@Override
	public String toString() {
		return "Scan [viewName=" + viewName + "]";
	}
	
}
