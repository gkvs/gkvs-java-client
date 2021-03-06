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

import javax.annotation.Nullable;

import rocks.gkvs.protos.ValueResult;
import rocks.gkvs.value.Nil;
import rocks.gkvs.value.Value;

/**
 * 
 * RecordError
 *
 * In case of returned error instead of Record
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public final class RecordError implements Record {

	private final @Nullable Key requestKey;
	private final ValueResult result;
	
	protected RecordError(@Nullable Key requestKey, ValueResult result) {
		this.requestKey = requestKey;
		this.result = result;
	}
	
	private void throwException() {
		
		rocks.gkvs.protos.Status status = result.getStatus();
		StringBuilder message = new StringBuilder();
		message.append(status.getCode().name());
		message.append(", errorCode=");
		message.append(status.getErrorCode());
		message.append(", errorMessage=");
		message.append(status.getErrorMessage());
		
		byte[] payload = result.toByteArray();
		
		throw new ResultException(message.toString(), status.getErrorDetails(), payload);
	}
	
	@Override
	public long tag() {
		return result.getHeader().getTag();
	}

	public String getStatus() {
		return result.getStatus().getCode().name();
	}
	
	public String getStatusDetails() {
		return result.getStatus().toString();
	}
	
	@Override
	public boolean exists() {
		throwException();
		return false;
	}

	@Override
	public int[] version() {
		throwException();
		return null;
	}

	@Override
	public int ttl() {
		throwException();
		return 0;
	}

	@Override
	public NullableKey key() {
		return new NullableKey(requestKey);
	}
	
	@Override
	public boolean hasValue() {
		return false;
	}
	
	@Override
	public Value value() {
		throwException();
		return Nil.get();
	}

	@Override
	public @Nullable byte[] rawValue() {
		return null;
	}

	@Override
	public String toString() {
		return "RECORD_ERROR [" + tag() + "]: " + getStatus();
	}

}
