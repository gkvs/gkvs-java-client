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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public final class GKVSConstants {

	private GKVSConstants() {
	}

	public static volatile Charset MUTABLE_KEY_CHARSET = StandardCharsets.US_ASCII;
	
	public static volatile Charset MUTABLE_VALUE_CHARSET = StandardCharsets.UTF_8;
	
	public static volatile String DEFAULT_SINGLE_VALUE_COLUMN = "";
	
}