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

import java.util.Map;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

public class PutTest extends AbstractClientTest {

	@Test(expected=IllegalArgumentException.class)
	public void testPutNull() {
		
		/**
		 * NULL values are not allowed
		 */
		
		GKVS.Client.put(TABLE, UUID.randomUUID().toString(), (String) null).sync();
	}
	
	@Test
	public void testPutGetRemove() {
		
		String key = UUID.randomUUID().toString();
		String value = "org";
		
		GKVS.Client.put(TABLE, key, value).sync();
		
		String actual = GKVS.Client.get(TABLE, key).sync().value().string();

		Assert.assertEquals(value, actual);
		
		GKVS.Client.remove(TABLE, key);
		
	}
	
	@Test
	public void testCompareAndPut() {
		
		String key = UUID.randomUUID().toString();
		String value = "org";
		String replaceValue = "replaced";
		
		
		GKVS.Client.put(TABLE, key, value).sync();
		
		Record record = GKVS.Client.get(TABLE, key).sync();
		
		Put put = GKVS.Client.put(TABLE, key, replaceValue);
		put.compareAndPut(0).sync();
		
		//System.out.println("result = " + put.result());
		
		// try with 0 version
		Assert.assertFalse(GKVS.Client.put(TABLE, key, replaceValue).compareAndPut(0).sync().updated());
		
		// try with unknown version
		Assert.assertFalse(GKVS.Client.put(TABLE, key, replaceValue).compareAndPut(435345234).sync().updated());
		
		// try with valid version
		Assert.assertTrue(GKVS.Client.put(TABLE, key, replaceValue).compareAndPut(record.version()).sync().updated());
		
		// check
		record = GKVS.Client.get(TABLE, key).sync();
		
		Assert.assertEquals(replaceValue, record.value().string());
	}
	
	@Test
	public void testPutSelect() {
		
		String key = UUID.randomUUID().toString();
		String column = "col";
		String value = "org";
		
		GKVS.Client.put(TABLE, key, column, value).sync();
		
		Record record = GKVS.Client.get(TABLE, key).sync();
		Assert.assertTrue(record.exists());
		
		Map<String, Value> values = record.valueMap();
		
		Assert.assertEquals(1, values.size());
		Assert.assertEquals(value, values.get(column).string());
		
		GKVS.Client.remove(TABLE, key).sync();
	}
	
	@Test
	public void testPutIfAbsent() {
	
		
		String key = UUID.randomUUID().toString();
		
		Assert.assertTrue(GKVS.Client.put(TABLE, key, "first").compareAndPut(0).async().getUnchecked().updated());
		
		Assert.assertEquals("first", GKVS.Client.get(TABLE, key).async().getUnchecked().value().string());
		
		Assert.assertFalse(GKVS.Client.put(TABLE, key, "second").compareAndPut(0).async().getUnchecked().updated());
		
		GKVS.Client.remove(TABLE, key).sync();
		
	}
	
	@Test
	public void testPutWithTTL() {
		
		String key = UUID.randomUUID().toString();
		
		GKVS.Client.put(TABLE, key, "value").withTtl(100).sync();
		
		Record rec = GKVS.Client.get(TABLE, key).metadataOnly().sync();
		
		Assert.assertTrue(rec.ttl() > 0);
		
	}
	
}
