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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

public class RemoveTest extends AbstractClientTest {
	
	
	@Test
	public void testRemoveSelect() {
		
		String key = UUID.randomUUID().toString();
		String column = "col";
		String value = "org";
		
		GKVS.Client.put(TABLE, key, column, value).sync();
		
		Record record = GKVS.Client.get(TABLE, key).select(column).sync();
		Assert.assertTrue(record.exists());
		
		Map<String, Value> values = record.valueMap();
		
		Assert.assertEquals(1, values.size());
		Assert.assertEquals(value, values.get(column).string());
		
		GKVS.Client.remove(TABLE, key).select(column).sync();
		
		
		Assert.assertFalse(GKVS.Client.get(TABLE, key).sync().exists());
		
	}
	
	@Test
	public void testRemoveSelectLeft() {
		
		String key = UUID.randomUUID().toString();
		String column = "col";
		String value = "org";
		
		String column2 = "col2";
		String value2 = "org2";
		
		GKVS.Client.putWithKey(TABLE, key)
			.put(column, value)
			.put(column2, value2)
			.sync();
		
		Get get = GKVS.Client.get(TABLE, key);
		
		Record record = get.sync();
		Assert.assertTrue(record.exists());
		
		Map<String, Value> values = record.valueMap();
		
		Assert.assertEquals(2, values.size());
		Assert.assertEquals(value, values.get(column).string());
		Assert.assertEquals(value2, values.get(column2).string());
		
		GKVS.Client.remove(TABLE, key).select(column).sync();
		
		Assert.assertTrue(GKVS.Client.get(TABLE, key).sync().exists());
		
		record = GKVS.Client.get(TABLE, key).sync();
		Assert.assertTrue(record.exists());
		
		values = record.valueMap();
		
		Assert.assertEquals(1, values.size());
		Assert.assertEquals(value2, values.get(column2).string());

		GKVS.Client.remove(TABLE, key).select(column2).sync();
		
		Assert.assertFalse(GKVS.Client.get(TABLE, key).sync().exists());
	}
	
	@Test
	public void testRemoveObserver() {
		
		String key = UUID.randomUUID().toString();
		
		Assert.assertFalse(remove(key));
		
		GKVS.Client.put(TABLE, key, "testRemoveObserver").sync();

		Assert.assertTrue(remove(key));
		
	}
	
	private boolean remove(String key) {
		
		BlockingCollector<Status> collector = new BlockingCollector<Status>();
		
		GKVS.Client.remove(TABLE, key).async(collector);
		
		List<Status> status = collector.awaitUnchecked();
		
		Assert.assertEquals(1, status.size());
		Assert.assertTrue(status.get(0) instanceof StatusSuccess);
		return status.get(0).updated();
		
	}
	
}
