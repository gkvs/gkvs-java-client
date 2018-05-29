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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.junit.AfterClass;
import org.junit.BeforeClass;

public abstract class AbstractClientTest {

	protected String TABLE = "TEST";

	protected static ExecutorService executor;
	
	@BeforeClass
	public static void setupClass() {
		
		executor = Executors.newCachedThreadPool();
		
	}
	
	@AfterClass
	public static void teardownClass() {
				
		executor.shutdownNow();
		
	}
}
