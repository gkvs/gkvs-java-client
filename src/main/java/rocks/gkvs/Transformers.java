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

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import io.grpc.stub.StreamObserver;
import rocks.gkvs.protos.ValueResult;

final class Transformers {

	private Transformers() {
	}
	
	protected static Record toRecord(ValueResult result) {
		if (RecordError.isError(result)) {
			return new RecordError(result);
		}
		else if (result.hasMetadata()) {
			return new RecordFound(result);
		}
		else {
			return new RecordNotFound(result);
		}
	}
	
	protected static Iterator<Record> toRecords(Iterator<ValueResult> iterator) {
		return Iterators.transform(iterator, ToRecordFn.INS);
	}
	
	private enum ToRecordFn implements Function<ValueResult, Record> {

		INS;

		public Record apply(ValueResult result) {
			return toRecord(result);
		}
		
	}
	
	protected static StreamObserver<ValueResult> observe(RecordObserver recordObserver) {
		return new StreamObserverAdapter(recordObserver);
	}
	
	protected static final class StreamObserverAdapter implements StreamObserver<ValueResult> {

		private final RecordObserver recordObserver;
		
		public StreamObserverAdapter(RecordObserver recordObserver) {
			this.recordObserver = recordObserver;
		}
		
		@Override
		public void onNext(ValueResult value) {
			recordObserver.onNext(Transformers.toRecord(value));
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
