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

import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * 
 * GkvsConstants
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public final class GkvsConstants {

	private GkvsConstants() {
	}

	public static volatile boolean USE_SINGLTON = true;
	
	public static volatile Charset MUTABLE_KEY_CHARSET = StandardCharsets.US_ASCII;
	
	public static volatile Charset MUTABLE_VALUE_CHARSET = StandardCharsets.UTF_8;
	
	public static volatile String DEFAULT_SINGLE_VALUE_COLUMN = "";
	
	public static volatile String GKVS_AUTH_CRT = "GkvsAuth.crt";
	
	public static final String CLASSPATH_PREFIX = "classpath:/";
	
	public static final int CLASSPATH_PREFIX_LENGTH = CLASSPATH_PREFIX.length();

	public enum SslProviderLib {
		
		JDK,
		OPENSSL,
		OPENSSL_REFCNT;
		
	}
	
	public static volatile SslProviderLib SSL_PROVIDER = SslProviderLib.OPENSSL;
	
}
