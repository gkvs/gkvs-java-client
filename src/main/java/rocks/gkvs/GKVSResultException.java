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

import rocks.gkvs.protos.Status;


public class GKVSResultException extends GKVSException {

	private static final long serialVersionUID = -4698988155155482009L;
	
	private final String errorDetails;
	
	public GKVSResultException(Status status) {
		super(status.getCode().name() + ", errorCode=" + status.getErrorCode() + ", errorMessage=" + status.getErrorMessage());
		this.errorDetails = status.getErrorDetails();
	}

	public String getErrorDetails() {
		return errorDetails;
	}

}
