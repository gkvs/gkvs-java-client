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

import java.util.concurrent.CountDownLatch;

public final class Observers {

	private Observers() {		
	}
	
	public static <T> Observer<T> empty() {
		
		return new Observer<T>() {

			@Override
			public void onNext(T item) {
			}

			@Override
			public void onError(Throwable t) {
			}

			@Override
			public void onCompleted() {
			}
			
		};
		
	}
	
	public static <T> Observer<T> empty(final CountDownLatch done) {
		
		return new Observer<T>() {

			@Override
			public void onNext(T item) {
			}

			@Override
			public void onError(Throwable t) {
				done.countDown();
			}

			@Override
			public void onCompleted() {
				done.countDown();
			}
			
		};
		
	}
	
	public static <T> Observer<T> console() {
		
		return new Observer<T>() {

			@Override
			public void onNext(T item) {
				System.out.println(item);
			}

			@Override
			public void onError(Throwable t) {
				System.err.println(t.getMessage());
				t.printStackTrace(System.err);
			}

			@Override
			public void onCompleted() {
			}
			
		};
		
	}
	
	
	public static <T> Observer<T> console(final CountDownLatch done) {
		
		return new Observer<T>() {

			@Override
			public void onNext(T item) {
				System.out.println(item);
			}

			@Override
			public void onError(Throwable t) {
				System.err.println(t.getMessage());
				t.printStackTrace(System.err);
				done.countDown();
			}

			@Override
			public void onCompleted() {
				done.countDown();
			}
			
		};
		
	}
	
	public static <I, O> Observer<I> transform(final Observer<O> out, final Fn<I, O> fn) {
		
		return new Observer<I>() {

			@Override
			public void onNext(I in) {
				try {
					out.onNext(fn.apply(in));
				}
				catch(Exception e) {
					out.onError(e);
				}
			}

			@Override
			public void onError(Throwable t) {
				out.onError(t);
			}

			@Override
			public void onCompleted() {
				out.onCompleted();
			}
			
		};
		
	}
	
	public interface Fn<I, O> {
		
		O apply(I in); 
		
	}
	
	public final static Fn<Record, Key> GET_KEY_FN = new Observers.Fn<Record, Key>() {

		@Override
		public Key apply(Record rec) {
			return rec.key().get();
		}
		
	};
	
}
