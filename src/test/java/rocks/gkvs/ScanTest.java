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
			Gkvs.Client.put(TABLE, key, "testScan").sync();
			LOAD_KEYS.add(key);
		}
		
	}
	
	@After
	public void teardown() {
		
		for (String key : LOAD_KEYS) {
			Gkvs.Client.remove(TABLE, key).sync();
		}
		
	}
	
	@Test
	public void testScan() {
		
		Set<String> notFoundKeys = new HashSet<>(LOAD_KEYS);
		
		Iterator<Record> records = Gkvs.Client.scan(TABLE).sync();
		
		while(records.hasNext()) {
			Record rec = records.next();
			
			try {
				String key = rec.key().getRecordKeyString();
				notFoundKeys.remove(key);
				//System.out.println("scan: " + rec);
			}
			catch(GkvsException e) {
				e.printStackTrace();
			}
		}
		
		Assert.assertTrue(notFoundKeys.isEmpty());
		
	}
	
	@Test
	public void testBucket() {
		
		Set<String> all = collectKeys(Gkvs.Client.scan(TABLE)
				.sync());
		
		Set<String> odd = collectKeys(Gkvs.Client.scan(TABLE)
				.withBucket(0, 2).sync());
		
		Set<String> even = collectKeys(Gkvs.Client.scan(TABLE)
				.withBucket(1, 2).sync());
		
		//System.out.println("all = " + all);
		//System.out.println("odd = " + odd);
		//System.out.println("even = " + even);
		
		Assert.assertEquals(all.size(), odd.size() + even.size());
		
		all.removeAll(odd);
		all.removeAll(even);
		
		Assert.assertTrue(all.isEmpty());
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
