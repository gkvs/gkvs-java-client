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

import rocks.gkvs.protos.OutputOptions;
import rocks.gkvs.protos.StatusCode;
import rocks.gkvs.protos.StatusResult;
import rocks.gkvs.protos.ValueResult;

/**
 * 
 * ProtocolUtils
 *
 * Serialization utils
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

final class ProtocolUtils {

	private ProtocolUtils() {
	}
	
	protected static OutputOptions getOutput(boolean includeKey, boolean includeValue) {
		
		if (includeKey) {
			
			if (includeValue) {
				return OutputOptions.KEYVALUE;
			}
			else {
				return OutputOptions.KEY;
			}
			
		}
		else {
			if (includeValue) {
				return OutputOptions.VALUE;
			}
			else {
				return OutputOptions.METADATA;
			}
			
		}
		
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
	
	protected static boolean isSuccess(StatusCode code) {
		switch(code) {
		case SUCCESS:
		case SUCCESS_NOT_UPDATED:
			return true;
		default:
			return false;
		}
	}
	
	
}
