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
package rocks.gkvs.value;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * TableTest
 *
 * @author Alex Shvid
 * @date Jun 30, 2018
 *
 */

public class TableTest extends AbstractValueTest {

	@Test
	public void testNull() {

		Table table = new Table();

		Assert.assertEquals(0, table.size());

		table.put(1, (Value) null);
		Assert.assertEquals(0, table.size());

		table.put(1, (String) null);
		Assert.assertEquals(0, table.size());

		table.put("1", (Value) null);
		Assert.assertEquals(0, table.size());

		table.put("1", (String) null);
		Assert.assertEquals(0, table.size());

	}

	@Test
	public void testEmpty() {

		Table table = new Table();

		Assert.assertEquals(TableType.EMPTY, table.getType());
		Assert.assertEquals(0, table.size());

		Table actual = Parser.parseTypedValue(table.toMsgpack());
		Assert.assertEquals(TableType.EMPTY, actual.getType());
		Assert.assertEquals(0, actual.size());

	}

	@Test
	public void testSingleInt() {

		Table table = new Table();

		table.put(123, new Num(123));

		Assert.assertEquals(TableType.LIST, table.getType());
		Assert.assertEquals(1, table.size());
		Assert.assertEquals("123", table.keySet().iterator().next());
		Assert.assertEquals(123, table.sortedKeys()[0]);

		Assert.assertEquals("817b7b", toHexString(table.toMsgpackValue()));
		Assert.assertEquals("817b7b", toHexString(table));

		Table actual = Parser.parseTypedValue(table.toMsgpack());

		Assert.assertEquals(TableType.LIST, actual.getType());
		Assert.assertEquals(1, actual.size());
		Assert.assertEquals("123", actual.keySet().iterator().next());
		Assert.assertEquals(123, actual.sortedKeys()[0]);

	}

	@Test
	public void testIntKey() {

		Table table = new Table();
		table.put(123, "stringValue");
		Assert.assertEquals(TableType.LIST, table.getType());
		table.put("123", "newStringValue");
		Assert.assertEquals(TableType.LIST, table.getType());
		Assert.assertEquals(1, table.size());
		Assert.assertEquals("newStringValue", table.get(123).asString());
	}

	@Test
	public void testIntToStringKey() {

		Table table = new Table();
		table.put(123, "stringValue");
		Assert.assertEquals(TableType.LIST, table.getType());
		table.put("abc", "newStringValue");
		Assert.assertEquals(TableType.MAP, table.getType());
		Assert.assertEquals(2, table.size());
		Assert.assertEquals("stringValue", table.get("123").asString());
		Assert.assertEquals("newStringValue", table.get("abc").asString());

	}

	@Test
	public void testIntArray() {

		Table table = new Table();
		Assert.assertEquals(-1, table.lastKey());
		Assert.assertTrue(table.keySet().isEmpty());
		Assert.assertEquals(0, table.sortedKeys().length);

		for (int i = 1; i != 11; ++i) {
			table.put(i, "value" + i);
		}

		Assert.assertEquals(TableType.LIST, table.getType());
		Assert.assertEquals(10, table.size());

		int[] keys = table.sortedKeys();
		for (int i = 1, j = 0; i != 11; ++i, ++j) {
			Assert.assertEquals(i, keys[j]);
		}

		Assert.assertEquals(10, table.lastKey());

	}

	@Test
	public void testIntToStringTable() {

		Table table = new Table();
		table.put("1", "one");
		table.put(2, "two");
		table.put("3", "three");

		Assert.assertEquals(TableType.LIST, table.getType());
		Assert.assertEquals(3, table.size());
		Assert.assertEquals(3, table.lastKey());

		byte[] msgpack = table.toMsgpack();
		Table actual = Parser.parseTypedValue(msgpack);

		Assert.assertEquals(TableType.LIST, actual.getType());
		Assert.assertEquals(3, actual.size());
		Assert.assertEquals(3, actual.lastKey());

		table.put("abc", "abc");

		Assert.assertEquals(TableType.MAP, table.getType());
		Assert.assertEquals(4, table.size());
		Assert.assertEquals(3, table.lastKey());

		msgpack = table.toMsgpack();
		actual = Parser.parseTypedValue(msgpack);

		Assert.assertEquals(TableType.MAP, actual.getType());
		Assert.assertEquals(4, actual.size());
		Assert.assertEquals(3, actual.lastKey());

	}

	@Test
	public void testStringGetters() {

		Table table = new Table();
		table.put("abc", "123.0");

		Assert.assertEquals(new Num(123.0), table.get("abc"));

		Assert.assertEquals(new Bool(true), table.get("abc").asBool());
		Assert.assertEquals(new Num(123.0), table.get("abc").asNum());
		Assert.assertEquals(new Str("123.0"), table.get("abc").asStr());

	}

	@Test
	public void testIntGetters() {

		Table table = new Table();
		table.put(5, "123.0");

		Assert.assertEquals(new Num(123.0), table.get(5));

		Assert.assertEquals(new Bool(true), table.get(5).asBool());
		Assert.assertEquals(new Num(123.0), table.get(5).asNum());
		Assert.assertEquals(new Str("123.0"), table.get(5).asStr());

		Assert.assertEquals(new Bool(true), table.get("5").asBool());
		Assert.assertEquals(new Num(123.0), table.get("5").asNum());
		Assert.assertEquals(new Str("123.0"), table.get("5").asStr());

	}

	@Test
	public void testInnerTable() {

		Table innerTable = new Table();
		innerTable.put("first", "Alex");

		Table table = new Table();
		table.put("name", innerTable);

		// System.out.println(table.toJson());

		byte[] msgpack = table.toMsgpack();
		Table actual = Parser.parseTypedValue(msgpack);

		// one entry in table guarantee the order
		Assert.assertEquals(table.toJson(), actual.toJson());

		Table name = actual.get("name").asTable();
		Assert.assertNotNull(name);
		Assert.assertEquals("Alex", name.get("first").asString());

	}
	
	@Test
	public void testNilValue() {
		
		Table tbl = new Table();
		tbl.put("name", "alex");
		
		Value val = tbl.get("nill_field");
		
		Assert.assertTrue(val.isNil());
		Assert.assertEquals(val, Nil.get());

		
	}

}
