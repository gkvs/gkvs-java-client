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
 * ParserTest
 *
 * @author Alex Shvid
 * @date Jun 30, 2018 
 *
 */

public class ParserTest {

	@Test
	public void testStringifyNull() {
		
		Parser.parseStringifyValue((String) null);
		
	}
	
	@Test
	public void testStringifyEmpty() {
		
		Value value = Parser.parseStringifyValue("");
		
		Assert.assertTrue(value instanceof Str);
		
		Str string = (Str) value;
		
		Assert.assertEquals("", string.asString());
	}
	
	@Test
	public void testStringifyZeroLong() {

		Value value = Parser.parseStringifyValue("0");
		
		Assert.assertTrue(value instanceof Num);
		
		Num val = (Num) value;
		
		Assert.assertEquals(NumType.INT64, val.getType());
		Assert.assertEquals(0, val.asLong());

	}
	
	@Test
	public void testStringifyZeroDouble() {

		Value value = Parser.parseStringifyValue("0.0");
		
		Assert.assertTrue(value instanceof Num);
		
		Num val = (Num) value;
		
		Assert.assertEquals(NumType.FLOAT64, val.getType());
		Assert.assertTrue(Math.abs(0.0 - val.asDouble()) < 0.00001);

	}
	
	@Test
	public void testStringifyLong() {

		Value value = Parser.parseStringifyValue("123456789");
		
		Assert.assertTrue(value instanceof Num);
		
		Num val = (Num) value;
		
		Assert.assertEquals(NumType.INT64, val.getType());
		Assert.assertEquals(123456789L, val.asLong());

	}
	
	
	@Test
	public void testStringifyString() {

		Value value = Parser.parseStringifyValue("abc");
		
		Assert.assertTrue(value instanceof Str);
		
		Str string = (Str) value;
		
		Assert.assertEquals("abc", string.asString());

	}
	
	@Test
	public void testNullExample() {
		
		Value value = Parser.parseValue((byte[]) null);
		
		Assert.assertNull(value);
		
	}
	
	@Test
	public void testEmptyExample() {
		
		final byte[] example =  new byte[] {};
		
		Value value = Parser.parseValue(example);
		
		Assert.assertNull(value);
		
	}
	
	@Test
	public void testExample() {
		
		final byte[] example =  new byte[] {-125, -93, 97, 99, 99, -93, 49, 50, 51, -90, 108, 111, 103, 105, 110, 115, -9, -92, 110, 97, 109, 101, -92, 65, 108, 101, 120};
	
		Value value = Parser.parseValue(example);
		
		Assert.assertNotNull(value);
		Assert.assertTrue(value instanceof Table);
		
		Table table = (Table) value;
		
		Assert.assertEquals("123", table.get("acc").asString());
		Assert.assertEquals(new Str("123"), table.get("acc").asStr());
		Assert.assertEquals(new Num(123), table.get("acc").asNum());
		
		Assert.assertEquals("Alex", table.get("name").asString());
		Assert.assertEquals(new Str("Alex"), table.get("name").asStr());
		
		Assert.assertEquals("-9", table.get("logins").asString());
		Assert.assertEquals(new Num(-9), table.get("logins").asNum());
		
		//System.out.println(table);
		
	}
	
	
}
