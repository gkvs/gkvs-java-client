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

import rocks.gkvs.value.Str;
import rocks.gkvs.value.Table;

/**
 * 
 * RemoveTest
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public class RemoveTest extends AbstractClientTest {
	
	@Test
	public void testRemoveSelect() {
		
		String key = UUID.randomUUID().toString();
		String column = "col";
		String value = "org";
		
		Table tbl = new Table();
		tbl.put(column, value);
		
		Gkvs.Client.put(STORE, key, tbl).sync();
		
		Record record = Gkvs.Client.get(STORE, key).select(column).sync();
		Assert.assertTrue(record.exists());
		
		Table actual = record.value().asTable();
		
		Assert.assertEquals(1, actual.size());
		Assert.assertEquals(value, actual.getStr(column).asString());
		
		Gkvs.Client.remove(STORE, key).select(column).sync();
		
		Assert.assertFalse(Gkvs.Client.get(STORE, key).sync().exists());
		
	}
	
	@Test
	public void testRemoveSelectLeft() {
		
		String key = UUID.randomUUID().toString();
		String column = "col";
		String value = "org";
		
		String column2 = "col2";
		String value2 = "org2";
		
		Table tbl = new Table();
		tbl.put(column, value);
		tbl.put(column2, value2);
		
		Gkvs.Client.put(STORE, key, tbl).sync();
		
		Get get = Gkvs.Client.get(STORE, key);
		
		Record record = get.sync();
		Assert.assertTrue(record.exists());
		
		Table actual = record.value().asTable();
		
		Assert.assertEquals(2, actual.size());
		Assert.assertEquals(value, actual.getStr(column).asString());
		Assert.assertEquals(value2, actual.getStr(column2).asString());
		
		Gkvs.Client.remove(STORE, key).select(column).sync();
		
		Assert.assertTrue(Gkvs.Client.get(STORE, key).sync().exists());
		
		record = Gkvs.Client.get(STORE, key).sync();
		Assert.assertTrue(record.exists());
		
		actual = record.value().asTable();
		
		Assert.assertEquals(1, actual.size());
		Assert.assertEquals(value2, actual.getStr(column2).asString());

		Gkvs.Client.remove(STORE, key).select(column2).sync();
		
		Assert.assertFalse(Gkvs.Client.get(STORE, key).sync().exists());
	}
	
	@Test
	public void testRemoveObserver() {
		
		String key = UUID.randomUUID().toString();
		
		Assert.assertFalse(remove(key));
		
		Gkvs.Client.put(STORE, key, new Str("testRemoveObserver")).sync();

		Assert.assertTrue(remove(key));
		
	}
	
	private boolean remove(String key) {
		
		BlockingCollector<Status> collector = new BlockingCollector<Status>();
		
		Gkvs.Client.remove(STORE, key).async(collector);
		
		List<Status> status = collector.awaitUnchecked();
		
		Assert.assertEquals(1, status.size());
		Assert.assertTrue(status.get(0) instanceof StatusSuccess);
		return status.get(0).updated();
		
	}
	
}
