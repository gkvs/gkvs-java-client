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

public enum RecordNotFound implements Record {
	
	RECORD_NOT_FOUND;

	@Override
	public boolean exists() {
		return false;
	}
	
	@Override
	public long version() {
		return 0;
	}

	@Override
	public int leftTtl() {
		return 0;
	}

	@Override
	public Key key() {
		return null;
	}

	@Override
	public byte[] value() {
		return null;
	}

	@Override
	public String valueAsString() {
		return null;
	}
	
	@Override
	public List<Cell> valueList() {
		return null;
	}

	@Override
	public Map<String, byte[]> valueMap() {
		return null;
	}
	
		
}