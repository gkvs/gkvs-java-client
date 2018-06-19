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

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * PutRemoveAllTest
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public class PutRemoveAllTest extends AbstractClientTest {

	@Test
	public void testPutAll() {
		
		Value def = Value.of("def");
		
		List<KeyValue> list = new ArrayList<KeyValue>();
		
		list.add(KeyValue.of(Key.raw(TABLE, UUID.randomUUID().toString()), def));
		list.add(KeyValue.of(Key.raw(TABLE, UUID.randomUUID().toString()), def));
		list.add(KeyValue.of(Key.raw(TABLE, UUID.randomUUID().toString()), def));
		
		Iterable<Status> result = Gkvs.Client.putAll().withTtl(10000).sync(list);
		
		for (Status status : result) {
			Assert.assertTrue(status.updated());
		}
		
		List<Key> keys = new ArrayList<Key>(list.size());
		for (KeyValue kv : list) {
			Assert.assertTrue(Gkvs.Client.exists(kv.key()).sync().exists());
			keys.add(kv.key());
		}

		result = Gkvs.Client.removeAll().sync(keys);
		
		for (Status status : result) {
			Assert.assertTrue(status.updated());
		}
		
		Iterable<Record> recs = Gkvs.Client.getAll().sync(keys);
		
		for (Record rec : recs) {
			Assert.assertTrue(rec instanceof RecordNotFound);
			Assert.assertFalse(rec.exists());
		}
	}
	
}
