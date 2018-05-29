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

import javax.annotation.Nullable;

import rocks.gkvs.protos.StatusCode;
import rocks.gkvs.protos.StatusResult;

public final class StatusError implements Status {

	private final @Nullable Key requestKey;
	private final StatusResult result;
	
	protected StatusError(@Nullable Key requestKey, StatusResult result) {
		this.requestKey = requestKey;
		this.result = result;
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
	
	@Override
	public long requestId() {
		return result.getRequestId();
	}

	@Override
	public boolean updated() {
		throwException();
		return false;
	}

	private void throwException() {
		throw new GKVSResultException(result.getStatus());
	}
	
	@Override
	public NullableKey key() {
		return new NullableKey(requestKey);
	}
	
	public String getStatus() {
		return result.getStatus().getCode().name();
	}
	
	public String getStatusDetails() {
		return result.getStatus().toString();
	}

	@Override
	public String toString() {
		return "STATUS_ERROR [" + requestId() + "]: " + getStatus();
	}
	
}