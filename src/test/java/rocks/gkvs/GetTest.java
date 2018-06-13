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

import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

public class GetTest extends AbstractClientTest {

	@Test
	public void testGet() {
		
		byte[] result = Gkvs.Client.get(TABLE, UUID.randomUUID().toString()).sync().value().bytes();
		
		Assert.assertNull("expected null result", result);
		
	}
	
	@Test
	public void testGetSelect() {
		
		String key = UUID.randomUUID().toString();
		String column = "col";
		String value = "org";
		
		Gkvs.Client.put(TABLE, key, column, value).sync();
		
		Record record = Gkvs.Client.get(TABLE, key).select(column).sync();
		Assert.assertTrue(record.exists());
		
		Map<String, Value> values = record.valueMap();
		
		Assert.assertEquals(1, values.size());
		Assert.assertEquals(value, values.get(column).string());
		
		Gkvs.Client.remove(TABLE, key).sync();
	}
	
	@Test
	public void testGetSelectObserver() {
		
		String key = UUID.randomUUID().toString();
		
		Gkvs.Client.put(TABLE, key, "column", "value").sync();
		
		BlockingCollector<Record> collector = new BlockingCollector<Record>();
		
		Gkvs.Client.get(TABLE, key).select("column").async(collector);
		
		List<Record> list = collector.awaitUnchecked();
		
		Assert.assertEquals(1, list.size());
		Assert.assertEquals("value", list.get(0).value().string());
		
		Gkvs.Client.remove(TABLE, key).sync();
	}
	
	//@Test
	public void testPerformanceGet() {
	
		long t0 = System.currentTimeMillis();
		
		for (int i = 0; i != 10000; ++i) {
			Gkvs.Client.get(TABLE, UUID.randomUUID().toString()).sync().value();
		}
		
		long diff = System.currentTimeMillis() - t0;
		
		System.out.println("10000 get requests in " + diff + " milliseconds");
	}
	
	
	
}
