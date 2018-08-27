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

/**
 * 
 * Observer
 *
 * Interface, used for async and sync communication between layers
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 * @param <T>
 */

public interface Observer<T> {
	
	/**
	 * Calls on next item
	 * 
	 * @param item
	 */
	
	void onNext(T item);
	
	/**
	 * Calls on error
	 * 
	 * @param t - exception
	 */
	
	void onError(Throwable t);
	
	/**
	 * Calls at the end of the stream
	 */
	
	void onCompleted();
	
}
