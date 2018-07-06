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
import java.util.Iterator;
import java.util.Set;
import java.util.UUID;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import rocks.gkvs.value.Str;

/**
 * 
 * ScanTest
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public class ScanTest extends AbstractClientTest {

	private final Set<String> LOAD_KEYS = new HashSet<>();
	
	@Before
	public void setup() {
		
		for (int i = 0; i != 10; ++i) {
			String key = UUID.randomUUID().toString();
			Gkvs.Client.put(TEST, key, new Str("testScan")).sync();
			LOAD_KEYS.add(key);
		}
		
	}
	
	@After
	public void teardown() {
		
		for (String key : LOAD_KEYS) {
			Gkvs.Client.remove(TEST, key).sync();
		}
		
	}
	
	@Test
	public void testScan() {
		
		Set<String> notFoundKeys = new HashSet<>(LOAD_KEYS);
		
		Iterator<Record> records = Gkvs.Client.scan(TEST).sync();
		
		while(records.hasNext()) {
			Record rec = records.next();
			
			try {
				String key = rec.key().getRecordKeyString();
				notFoundKeys.remove(key);
			}
			catch(GkvsException e) {
				e.printStackTrace();
			}
		}
		
		Assert.assertTrue(notFoundKeys.isEmpty());
		
	}
	
	protected static Set<String> collectKeys(Iterator<Record> records) {
		
		Set<String> set = new HashSet<>();
		
		while(records.hasNext()) {
			Record rec = records.next();
			
			try {
				String key = rec.key().getRecordKeyString();
				set.add(key);
			}
			catch(GkvsException e) {
				e.printStackTrace();
			}
		}
		
		return set;
		
	}
	
	
}
