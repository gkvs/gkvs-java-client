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

import java.util.ArrayList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.collect.Iterators;

public class BlockingCollector<T> implements Observer<T> {

	private final Queue<T> collector = new ConcurrentLinkedQueue<>();
	private final CountDownLatch done = new CountDownLatch(1);
	
	private final AtomicReference<Throwable> exception = new AtomicReference<>(null);
	
	@Override
	public void onNext(T record) {
		collector.add(record);
	}
	
	@Override
	public void onError(Throwable t) {
		exception.set(t);
		done.countDown();
	}
	
	@Override
	public void onCompleted() {
		done.countDown();
	}
	
	public List<T> await() throws InterruptedException {
		done.await();
		return result();
	}
	
	public List<T> awaitUnchecked() {
		try {
			done.await();
		} catch (InterruptedException e) {
			throw new GkvsException("interrupted await", e);
		}
		return result();
	}

	public List<T> await(long timeout, TimeUnit unit) throws InterruptedException {
		done.await(timeout, unit);
		return result();
	}
	
	public List<T> awaitUnchecked(long timeout, TimeUnit unit) {
		try {
			done.await(timeout, unit);
		} catch (InterruptedException e) {
			throw new GkvsException("interrupted await", e);
		}
		return result();
	}
	
	private List<T> result() {
		Throwable t = exception.get();
		if (t != null) {
			throw new GkvsException("result error", t);
		}
		
		List<T> list = new ArrayList<T>(collector.size());
		Iterators.addAll(list, collector.iterator());
		return list;
	}
}
