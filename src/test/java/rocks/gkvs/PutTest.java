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

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

public class PutTest extends AbstractClientTest {

	@Test(expected=IllegalArgumentException.class)
	public void testPutNull() {
		
		/**
		 * If you want to remove value, please use remove method
		 */
		
		GKVS.Client.put(TABLE, UUID.randomUUID().toString(), (String) null).sync();
	}
	
	@Test
	public void testPutGetRemove() {
		
		String key = UUID.randomUUID().toString();
		String value = "org";
		
		GKVS.Client.put(TABLE, key, value).sync();
		
		String actual = GKVS.Client.get(TABLE, key).sync().valueAsString();

		Assert.assertEquals(value, actual);
		
		GKVS.Client.remove(TABLE, key);
		
	}
	
}
