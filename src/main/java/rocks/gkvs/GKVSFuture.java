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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.MoreExecutors;

public class GKVSFuture<T> implements Future<T> {

	private final ListenableFuture<T> delegate; 
	
	protected GKVSFuture(ListenableFuture<T> delegate) {
		this.delegate = delegate;
	}
	
	@Override
	public boolean cancel(boolean mayInterruptIfRunning) {
		return delegate.cancel(mayInterruptIfRunning);
	}

	@Override
	public boolean isCancelled() {
		return delegate.isCancelled();
	}

	@Override
	public boolean isDone() {
		return delegate.isDone();
	}

	@Override
	public T get() throws InterruptedException, ExecutionException {
		return delegate.get();
	}
	
	public T getUnchecked() {
		try {
			return delegate.get();
		} catch (InterruptedException | ExecutionException e) {
			throw new GKVSException("future exception", e);
		}
	}

	@Override
	public T get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
		return delegate.get(timeout, unit);
	}
	
	public T getUnchecked(long timeout, TimeUnit unit) {
		try {
			return delegate.get(timeout, unit);
		} catch (InterruptedException | ExecutionException | TimeoutException e) {
			throw new GKVSException("future exception", e);
		}
	}
	
	public void addListener(Runnable listener) {
		delegate.addListener(listener, MoreExecutors.directExecutor());	
	}
	
	public void addListener(Runnable listener, Executor executor) {
		delegate.addListener(listener, executor);	
	}
	
}
