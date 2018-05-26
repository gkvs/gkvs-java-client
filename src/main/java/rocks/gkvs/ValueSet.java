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

import java.util.HashMap;
import java.util.Map;

public class ValueSet {

	private Map<String, byte[]> values = new HashMap<String, byte[]>();
	
	public ValueSet() {
	}
	
	public Map<String, byte[]> map() {
		return values;
	}
	
	public byte[] single() {
		if (values.size() != 1) {
			throw new IllegalArgumentException("expected a single value");
		}
		return values.entrySet().iterator().next().getValue();
	}
	
}
