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
import java.util.concurrent.atomic.AtomicBoolean;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GetFutureTest extends AbstractClientTest {

	private Key KEY = Key.raw(TABLE, UUID.randomUUID().toString());
	
	
	@Before
	public void setup() {

		GKVS.Client.put(KEY, Value.of("GetFutureTest")).sync();
		
	}
	
	@After
	public void teardown() {
		
		GKVS.Client.remove(KEY).sync();
		
	}
	
	@Test
	public void testGetFuture() {
		
		final AtomicBoolean triggered = new AtomicBoolean(false);
		RecordFuture future = GKVS.Client.get(KEY).async();

		future.addListener(new Runnable() {

			@Override
			public void run() {
				triggered.set(true);
			}
			
		}, executor);
		
		Record rec = future.getUnchecked();
		
		Assert.assertNotNull(rec);
		Assert.assertEquals("GetFutureTest", rec.value().string());
		Assert.assertTrue(triggered.get());
	}
	
	
}
