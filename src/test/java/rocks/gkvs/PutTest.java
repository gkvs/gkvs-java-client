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

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import rocks.gkvs.value.Str;
import rocks.gkvs.value.Table;

/**
 * 
 * PutTest
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public class PutTest extends AbstractClientTest {

	@Test(expected=GkvsException.class)
	public void testPutNull() {
		
		/**
		 * NULL values are not allowed
		 */
		
		Gkvs.Client.put(TEST, "key", null).sync();
	}
	
	@Test
	public void testPutGetRemove() throws Exception {
		
		String key = UUID.randomUUID().toString();
		
		Table tbl = new Table();
		tbl.put("field", "value");
		
		Gkvs.Client.put(TEST, key, tbl).sync().updated();
		
		Table actual = Gkvs.Client.get(TEST, key).sync().value().asTable();
		
		Assert.assertEquals("value", actual.get("field").asString());
		
		Gkvs.Client.remove(TEST, key);
		
	}
	
	@Test
	public void testCompareAndPut() {
		
		String key = UUID.randomUUID().toString();
		
		Table tbl = new Table();
		tbl.put("field", "org");
		
		Gkvs.Client.put(TEST, key, tbl).sync();
		
		Record record = Gkvs.Client.get(TEST, key).sync();
		Table first = record.value().asTable();
		Assert.assertEquals("org", first.get("field").asString());
		
		Table replaceTbl = new Table();
		replaceTbl.put("field", "replace");

		// try with 0 version
		boolean updated = Gkvs.Client.compareAndPut(TEST, key, replaceTbl, null).sync().updated();
		Assert.assertFalse(updated);
		
		// try with unknown version
		updated = Gkvs.Client.compareAndPut(TEST, key, replaceTbl, new int[] {435345234}).sync().updated();
		Assert.assertFalse(updated);
		
		// try with valid version
		updated = Gkvs.Client.compareAndPut(TEST, key, replaceTbl, record.version()).sync().updated();
		Assert.assertTrue(updated);
		
		// check
		String actualValue = Gkvs.Client.get(TEST, key).sync().value().asTable().get("field").asString();
		
		Assert.assertEquals("replace", actualValue);
		
	}
	
	@Test
	public void testPutSelect() {
		
		String key = UUID.randomUUID().toString();
		String column = "col";
		String value = "org";
		
		Table tbl = new Table();
		tbl.put(column, value);
		
		Gkvs.Client.put(TEST, key, tbl).sync();
		
		Record record = Gkvs.Client.get(TEST, key).sync();
		Assert.assertTrue(record.exists());
		
		Table actualTbl = record.value().asTable();
		
		Assert.assertEquals(1, actualTbl.size());
		Assert.assertEquals(value, actualTbl.get(column).asString());
		
		Gkvs.Client.remove(TEST, key).sync();
	}
	
	@Test
	public void testPutIfAbsent() {
	
		String key = UUID.randomUUID().toString();
		
		Assert.assertTrue(Gkvs.Client.putIfAbsent(TEST, key, new Str("first")).async().getUnchecked().updated());
		
		Assert.assertEquals("first", Gkvs.Client.get(TEST, key).async().getUnchecked().value().asStr().asString());
		
		Assert.assertFalse(Gkvs.Client.putIfAbsent(TEST, key, new Str("second")).async().getUnchecked().updated());
		
		Gkvs.Client.remove(TEST, key).sync();
		
	}
	
	@Test
	public void testPutWithTTL() {
		
		String key = UUID.randomUUID().toString();
		
		Gkvs.Client.put(TEST, key, new Str("value")).withTtl(100).sync();
		
		Record rec = Gkvs.Client.get(TEST, key).metadataOnly().sync();
		
		System.out.println("rec.ttl = " + rec.ttl());
		
		Assert.assertTrue(rec.ttl() > 0);
		
	}
	
}
