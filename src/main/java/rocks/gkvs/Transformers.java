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

import javax.annotation.Nullable;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import io.grpc.stub.StreamObserver;
import rocks.gkvs.protos.ValueResult;

final class Transformers {

	private Transformers() {
	}
	
	protected static Record toRecord(@Nullable Key requestKey, ValueResult result) {
		if (RecordError.isError(result)) {
			return new RecordError(requestKey, result);
		}
		else if (result.hasMetadata()) {
			return new RecordFound(requestKey, result);
		}
		else {
			return new RecordNotFound(requestKey, result);
		}
	}
	
	protected static Iterator<Record> toRecords(Iterator<ValueResult> iterator) {
		return Iterators.transform(iterator, SimpleRecordFn.INS);
	}
	
	protected static Iterator<Record> toRecords(Iterator<ValueResult> iterator, KeyResolver keyResolver) {
		return Iterators.transform(iterator, new RecordFn(keyResolver));
	}
	
	protected interface KeyResolver {
		
		@Nullable Key find(long requestId);
		
	}
	
	protected enum SimpleRecordFn implements Function<ValueResult, Record> {

		INS;
		
		public Record apply(ValueResult result) {
			return toRecord(null, result);
		}
		
	}
	
	protected static final class RecordFn implements Function<ValueResult, Record> {

		private final KeyResolver keyResolver;
		
		public RecordFn(KeyResolver keyResolver) {
			this.keyResolver = keyResolver;
		}

		public Record apply(ValueResult result) {
			return toRecord(keyResolver.find(result.getRequestId()), result);
		}
		
	}
	
	protected static StreamObserver<ValueResult> observe(RecordObserver recordObserver, KeyResolver keyResolver) {
		return new StreamObserverAdapter(recordObserver, keyResolver);
	}
	
	protected static final class StreamObserverAdapter implements StreamObserver<ValueResult> {

		private final RecordObserver recordObserver;
		private final KeyResolver keyResolver;
		
		public StreamObserverAdapter(RecordObserver recordObserver, KeyResolver keyResolver) {
			this.recordObserver = recordObserver;
			this.keyResolver = keyResolver;
		}
		
		@Override
		public void onNext(ValueResult value) {
			recordObserver.onNext(Transformers.toRecord(keyResolver.find(value.getRequestId()), value));
		}

		@Override
		public void onError(Throwable t) {
			recordObserver.onError(t);
		}

		@Override
		public void onCompleted() {
			recordObserver.onCompleted();
		}
		
	}
	
}
