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

import rocks.gkvs.protos.StatusResult;

/**
 * 
 * StatusSuccess
 *
 * Successful operation
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public final class StatusSuccess implements Status {

	private final @Nullable Key requestKey;
	private final StatusResult result;

	protected StatusSuccess(@Nullable Key requestKey, StatusResult result) {
		this.requestKey = requestKey;
		this.result = result;
	}

	@Override
	public long tag() {
		return result.getHeader().getTag();
	}

	@Override
	public boolean updated() {
		switch (result.getStatus().getCode()) {
		case SUCCESS:
			return true;
		case SUCCESS_NOT_UPDATED:
			return false;
		default:
			throw new GkvsException("invalid status code: " + result);
		}
	}

	@Override
	public NullableKey key() {
		return new NullableKey(requestKey);
	}
	
	@Override
	public String toString() {
		return "STATUS_SUCCESS [" + tag() + "]: " + key();
	}

}
