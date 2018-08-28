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
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;

import com.google.common.collect.Lists;

import reactor.core.publisher.Flux;
import rocks.gkvs.value.Str;
import rx.Observable;

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
	public void testScanSync() {
		
		Iterator<Record> records = Gkvs.Client.scan(TEST).sync();
				
		Assert.assertTrue(filterKeys(records).isEmpty());
		
	}
	
	@Test
	public void testScanAsync() throws InterruptedException {
		
		BlockingCollector<Record> records = new BlockingCollector<Record>();
		
		Gkvs.Client.scan(TEST).async(records);
		
		List<Record> list = records.await();

		Assert.assertTrue(filterKeys(list.iterator()).isEmpty());

	}
	
	
	@Test
	public void testScanObserve() throws InterruptedException {
				
		Observable<Record> actual = Gkvs.Client.scan(TEST).observe();
		
		Iterable<Record> result = actual.toBlocking().toIterable();
		
		List<Record> list = Lists.newArrayList(result);

		Assert.assertTrue(filterKeys(list.iterator()).isEmpty());

	}

	@Test
	public void testScanFluxIterable() throws InterruptedException {
				
		Flux<Record> actual = Gkvs.Client.scan(TEST).flux();
		
		Iterable<Record> result = actual.toIterable();
		
		List<Record> list = Lists.newArrayList(result);

		Assert.assertTrue(filterKeys(list.iterator()).isEmpty());

	}
	
	@Test
	public void testScanFluxBlock() throws InterruptedException {
				
		Flux<Record> actual = Gkvs.Client.scan(TEST).flux();
		
		List<Record> list = actual.collectList().block();
		
		Assert.assertTrue(filterKeys(list.iterator()).isEmpty());

	}
	
	@Test
	public void testScanFluxSubscribe() throws InterruptedException {
				
		Flux<Record> actual = Gkvs.Client.scan(TEST).flux();
		
		final AtomicReference<Throwable> error = new AtomicReference<>();
		final CountDownLatch done = new CountDownLatch(1);
		final List<Record> list = new CopyOnWriteArrayList<Record>();
		
		
		actual.subscribe(new Subscriber<Record>() {
	
			@Override
			public void onSubscribe(Subscription s) {
				s.request(Long.MAX_VALUE);
			}

			@Override
			public void onNext(Record record) {
				list.add(record);
			}

			@Override
			public void onError(Throwable t) {
				error.set(t);
			}

			@Override
			public void onComplete() {
				done.countDown();
			}


		});
		
		done.await();
		
		if (error.get() != null) {
			throw new RuntimeException("flux error:", error.get());
		}
		
		Assert.assertTrue(filterKeys(list.iterator()).isEmpty());

	}
	
	
	private Set<String> filterKeys(Iterator<Record> records) {
		Set<String> notFoundKeys = new HashSet<>(LOAD_KEYS);
		
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
		return notFoundKeys;
	}
	

	
	
}
