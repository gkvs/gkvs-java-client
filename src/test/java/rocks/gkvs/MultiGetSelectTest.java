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

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import rocks.gkvs.value.Table;

/**
 * 
 * MultiGetSelectTest
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public class MultiGetSelectTest extends AbstractClientTest {

	private final Set<Key> LOAD_KEYS = new HashSet<>();
	
	@Before
	public void setup() {
		
		for (int i = 0; i != 10; ++i) {
			Key key = Key.raw(TEST, UUID.randomUUID().toString());
			
			Table tbl = new Table();
			tbl.put("col", "MultiGetSelectTest");
			
			Gkvs.Client.put(key, tbl).sync();
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
	public void testMultiGetSelect() {

		Set<Key> notFoundKeys = new HashSet<>(LOAD_KEYS);
		
		Iterable<Record> records = Gkvs.Client.multiGet(LOAD_KEYS).select("col").sync();
		
		for (Record rec : records) {
			notFoundKeys.remove(rec.key().get());
		}

		Assert.assertTrue(notFoundKeys.isEmpty());
	}
	
	
}
