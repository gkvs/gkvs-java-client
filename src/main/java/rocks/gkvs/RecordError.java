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

import java.util.List;
import java.util.Map;

import rocks.gkvs.protos.Status;
import rocks.gkvs.protos.StatusCode;
import rocks.gkvs.protos.StatusResult;
import rocks.gkvs.protos.ValueResult;

public final class RecordError implements Record {

	private final Status status;
	private final long requestId;
	
	protected RecordError(ValueResult valueResult) {
		this.status = valueResult.getStatus();
		this.requestId = valueResult.getRequestId();
	}
	
	protected static boolean isError(ValueResult valueResult) {
		if (valueResult.hasStatus()) {
			return !isSuccess(valueResult.getStatus().getCode());
		}
		return true;
	}
	
	protected static boolean isError(StatusResult statusResult) {
		if (statusResult.hasStatus()) {
			return !isSuccess(statusResult.getStatus().getCode());
		}
		return true;
	}
	
	public static boolean isSuccess(StatusCode code) {
		switch(code) {
		case SUCCESS:
		case SUCCESS_NOT_UPDATED:
		case SUCCESS_END_STREAM:
			return true;
		default:
			return false;
		}
	}
	
	private void throwException() {
		throw new GKVSResultException(status);
	}
	
	@Override
	public long requestId() {
		return requestId;
	}

	public String getStatus() {
		return status.toString();
	}
	
	@Override
	public boolean exists() {
		throwException();
		return false;
	}

	@Override
	public long version() {
		throwException();
		return 0;
	}

	@Override
	public int ttl() {
		throwException();
		return 0;
	}

	@Override
	public NullableKey key() {
		throwException();
		return null;
	}

	@Override
	public NullableValue value() {
		throwException();
		return null;
	}

	@Override
	public List<Value> valueList() {
		throwException();
		return null;
	}

	@Override
	public Map<String, Value> valueMap() {
		throwException();
		return null;
	}

	@Override
	public String toString() {
		return "RECORD_ERROR [" + requestId() + "]: " + status.getCode().name();
	}

}
