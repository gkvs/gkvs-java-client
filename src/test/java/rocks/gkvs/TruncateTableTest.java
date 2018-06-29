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

import java.util.Iterator;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import org.junit.Test;

import rocks.gkvs.value.Str;

/**
 * 
 * TruncateTableTest
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public class TruncateTableTest extends AbstractClientTest {

	@Test
	public void testEmpty() {
		
	}
	
	//@Test
	public void cleanUp() {
		
		Iterator<Record> i = Gkvs.Client.scan(TEST)
				.sync();
		
		while(i.hasNext()) {
			
			try {
				Gkvs.Client.remove(i.next().key().get()).sync();
			}
			catch(GkvsException e) {
				e.printStackTrace();
			}
			
		}
		
	}
	
	//@Test
	public void cleanUpStream() throws InterruptedException {
		
		for (int i = 0; i != 10; ++i) {
			String key = UUID.randomUUID().toString();
			Gkvs.Client.put(TEST, key, new Str("TruncateTableTest")).sync();
		}
		
		CountDownLatch done = new CountDownLatch(1);
		
		Observer<Key> key = Gkvs.Client.removeAll().async(Observers.<Status>console(done));
				
		Observer<Record> record = Observers.transform(key, Observers.GET_KEY_FN);
		
		Gkvs.Client.scan(TEST).async(record);
		
		// await tota async process
		done.await();
	}
	
}
