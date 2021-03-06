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

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.google.common.collect.Lists;

import reactor.core.publisher.Flux;
import rocks.gkvs.value.Str;
import rx.Observable;

/**
 * 
 * GetAllTest
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public class GetAllTest extends AbstractClientTest {

	private final Set<Key> LOAD_KEYS = new HashSet<>();
	
	@Before
	public void setup() {
		
		for (int i = 0; i != 10; ++i) {
			Key key = Key.raw(TEST, UUID.randomUUID().toString());
			
			Gkvs.Client.put(key, new Str("GetAllTest")).sync();
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
	public void testGetAllSync() {

		Iterable<Record> actual = Gkvs.Client.getAll().sync(LOAD_KEYS);

		List<Record> list = (List<Record>) actual;
		
		Assert.assertEquals(list.size(), LOAD_KEYS.size());
		
	}
	
	@Test
	public void testGetAllAsync() throws InterruptedException {

		BlockingCollector<Record> records = new BlockingCollector<Record>();
		
		Observer<Key> keys = Gkvs.Client.getAll().async(records);
		
		for (Key key : LOAD_KEYS) {
			keys.onNext(key);
		}
		keys.onCompleted();
		
		List<Record> list = records.await();
		
		Assert.assertEquals(list.size(), LOAD_KEYS.size());
		
	}
	
	
	@Test
	public void testGetAllObserver() {

		Observable<Record> actual = Gkvs.Client.getAll().observe(Observable.from(LOAD_KEYS));
		
		Iterable<Record> result = actual.toBlocking().toIterable();
		
		List<Record> list = Lists.newArrayList(result);
		
		Assert.assertEquals(list.size(), LOAD_KEYS.size());
		
	}
	
	@Test
	public void testGetAllFluxIterable() {

		Flux<Record> actual = Gkvs.Client.getAll().flux(Flux.fromIterable(LOAD_KEYS));
		
		Iterable<Record> result = actual.toIterable();
		List<Record> list = Lists.newArrayList(result);
				
		Assert.assertEquals(list.size(), LOAD_KEYS.size());
		
	}
	
	@Test
	public void testGetAllFluxBlock() {

		Flux<Record> actual = Gkvs.Client.getAll().flux(Flux.fromIterable(LOAD_KEYS));
	
		List<Record> list = actual.collectList().block();
		
		Assert.assertEquals(list.size(), LOAD_KEYS.size());
		
	}
	
}
