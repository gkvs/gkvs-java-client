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

import java.util.Iterator;

import rocks.gkvs.protos.Bucket;
import rocks.gkvs.protos.OutputOptions;
import rocks.gkvs.protos.RequestOptions;
import rocks.gkvs.protos.ScanOperation;
import rocks.gkvs.protos.Select;
import rocks.gkvs.protos.ValueResult;

public final class Scan {

	private enum ValueType {
		RAW, DIGEST, MAP;
	}
	
	private final GKVSClient instance;

	private String tableName;
	private RequestOptions.Builder optionsOrNull;
	private Select.Builder selectOrNull;
	private Bucket.Builder bucketOrNull;
	
	private boolean includeKey = true;
	private boolean includeValue = false;
	private ValueType valueType = ValueType.RAW;
	
	
	public Scan(GKVSClient instance) {
		this.instance = instance;
	}
	
	public Scan table(String tableName) {
		this.tableName = tableName;
		return this;
	}
	
	public Scan withPit(long pit) {
		if (optionsOrNull == null) {
			optionsOrNull = RequestOptions.newBuilder();
		}
		optionsOrNull.setPit(pit);
		return this;
	}
	
	public Scan withBucket(int n, int total) {
		if (n < 0 || total < 0 || n >= total) {
			throw new IllegalArgumentException("invalid bucket number and total number of buckets");
		}
			
		if (bucketOrNull == null) {
			bucketOrNull = Bucket.newBuilder();
		}

		bucketOrNull.setBucketNum(n);
		bucketOrNull.setTotalNum(total);
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
	
	public Scan valueRaw() {
		this.valueType = ValueType.RAW;
		return this;
	}
	
	public Scan valueDigest() {
		this.valueType = ValueType.DIGEST;
		return this;
	}

	public Scan valueMap() {
		this.valueType = ValueType.MAP;
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
	
	public Iterator<Record> sync() {
		
		ScanOperation.Builder builder = ScanOperation.newBuilder();
		
		if (tableName == null) {
			throw new IllegalArgumentException("table name is null");
		}
		
		builder.setTableName(tableName);
		builder.setOutput(output());
		
		if (optionsOrNull != null) {
			builder.setOptions(optionsOrNull);
		}
		
		if (selectOrNull != null) {
			builder.setSelect(selectOrNull);
		}
		
		if (bucketOrNull != null) {
			builder.setBucket(bucketOrNull);
		}
		
		System.out.println(builder.build());
		
		Iterator<ValueResult> results = instance.getBlockingStub().scan(builder.build());
		return Transformers.toRecords(results);
		
	}
	
	private OutputOptions output() {
		
		if (includeKey) {
			
			if (includeValue) {
				switch(valueType) {
				case RAW:
					return OutputOptions.KEY_VALUE_RAW;
				case DIGEST:
					return OutputOptions.KEY_VALUE_DIGEST;
				case MAP:
					return OutputOptions.KEY_VALUE_MAP;
				}
				return OutputOptions.KEY_VALUE_RAW;
			}
			else {
				return OutputOptions.KEY;
			}
			
		}
		else {
			if (includeValue) {
				switch(valueType) {
					case RAW:
						return OutputOptions.VALUE_RAW;
					case DIGEST:
						return OutputOptions.VALUE_DIGEST;
					case MAP:
						return OutputOptions.VALUE_MAP;
					}
				return OutputOptions.VALUE_RAW;
			}
			else {
				return OutputOptions.METADATA_ONLY;
			}
			
		}
		
	}

	
}
