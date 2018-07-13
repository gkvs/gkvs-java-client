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
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import rocks.gkvs.value.Table;
import rocks.gkvs.value.Value;

/**
 * 
 * GetTest
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public class GetTest extends AbstractClientTest {

	@Test
	public void testGet() {
		
		Value result = Gkvs.Client.get(TEST, UUID.randomUUID().toString()).sync().value();
		
		Assert.assertTrue("expected null result", result.isNil());
		
	}
	
	@Test
	public void testGetSelect() {
		
		String key = UUID.randomUUID().toString();
		String column = "col";
		String value = "org";
		
		Table tbl = new Table();
		tbl.put(column, value);
		
		Gkvs.Client.put(TEST, key, tbl).sync();
		
		Record record = Gkvs.Client.get(TEST, key).select(column).sync();
		Assert.assertTrue(record.exists());
		
		Table actual = record.value().asTable();
		
		Assert.assertEquals(1, actual.size());
		Assert.assertEquals(value, actual.get(column).asString());
		
		Gkvs.Client.remove(TEST, key).sync();
	}
	
	@Test
	public void testGetSelectObserver() {
		
		String key = UUID.randomUUID().toString();
		
		Table tbl = new Table();
		tbl.put("column", "value");
		
		Gkvs.Client.put(TEST, key, tbl).sync();
		
		BlockingCollector<Record> collector = new BlockingCollector<Record>();
		
		Gkvs.Client.get(TEST, key).select("column").async(collector);
		
		List<Record> list = collector.awaitUnchecked();
		
		Assert.assertEquals(1, list.size());
		Record record = list.get(0);
		Table actual = record.value().asTable();
		Assert.assertEquals("value", actual.get("column").asString());
		
		Gkvs.Client.remove(TEST, key).sync();
	}
	
	
}
