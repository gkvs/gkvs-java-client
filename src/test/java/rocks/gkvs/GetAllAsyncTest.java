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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * 
 * GetAllAsyncTest
 *
 * Load keys 
 * Gets all keys
 * Remove keys
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public class GetAllAsyncTest extends AbstractClientTest {

	private final Set<Key> LOAD_KEYS = new HashSet<>();
	
	@Before
	public void setup() {
		
		for (int i = 0; i != 10; ++i) {
			Key key = Key.raw(TABLE, UUID.randomUUID().toString());
			
			Gkvs.Client.put(key, Value.of("GetAllTest")).sync();
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
	public void testGetAll() {

		final List<Record> list = new CopyOnWriteArrayList<>();

		final CountDownLatch done = new CountDownLatch(1);
		Observer<Key> keys = Gkvs.Client.getAll().async(new Observer<Record>() {
			
			@Override
			public void onNext(Record record) {
				list.add(record);
			}

			@Override
			public void onError(Throwable t) {
				t.printStackTrace();
				done.countDown();
			}

			@Override
			public void onCompleted() {
				done.countDown();
			}
			
		});
		
		for (Key key : LOAD_KEYS) {
			keys.onNext(key);
			try {
				Thread.sleep(1);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}	
		keys.onCompleted();

		try {
			done.await();
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
		
		Assert.assertEquals(list.size(), LOAD_KEYS.size());
		
	}
	
}
