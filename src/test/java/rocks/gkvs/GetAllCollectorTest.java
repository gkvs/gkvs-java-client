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

import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class GetAllCollectorTest extends AbstractClientTest {

	private final Set<Key> LOAD_KEYS = new HashSet<>();
	
	@Before
	public void setup() {
		
		for (int i = 0; i != 10; ++i) {
			Key key = Key.raw(TABLE, UUID.randomUUID().toString());
			
			Gkvs.Client.put(key, Value.of("GetAllCollectorTest")).sync();
			LOAD_KEYS.add(key);
		}
		
	}
	
	@After
	public void teardown() {
		
		for (Key key : LOAD_KEYS) {
			Gkvs.Client.remove(key).sync();
		}
		
	}
	
	@Test
	public void testGetAllCollector() {
	
		BlockingCollector<Record> collector = new BlockingCollector<Record>();
		Observer<Key> keys = Gkvs.Client.getAll().async(collector);
		
		for (Key key : LOAD_KEYS) {
			keys.onNext(key);
		}	
		keys.onCompleted();
		
		
		List<Record> list = collector.awaitUnchecked();
		Assert.assertEquals(list.size(), LOAD_KEYS.size());
	}
}
