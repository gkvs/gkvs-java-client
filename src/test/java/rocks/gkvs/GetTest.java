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

public class GetTest extends AbstractClientTest {

	@Test
	public void testGet() {
		
		byte[] result = GKVS.Client.get("TEST", UUID.randomUUID().toString()).sync().value();
		
		Assert.assertNull("expected null result", result);
		
	}
	
	
	@Test
	public void testPerformanceGet() {
	
		long t0 = System.currentTimeMillis();
		
		for (int i = 0; i != 10000; ++i) {
			GKVS.Client.get("TEST", UUID.randomUUID().toString()).sync().value();
		}
		
		long diff = System.currentTimeMillis() - t0;
		
		System.out.println("10000 get requests in " + diff + " milliseconds");
	}
	
	
	
}
