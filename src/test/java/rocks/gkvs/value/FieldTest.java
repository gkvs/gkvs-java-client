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
 * FieldTest
 *
 * @author Alex Shvid
 * @date Jun 30, 2018 
 *
 */

public class FieldTest {

	@Test(expected=IllegalArgumentException.class)
	public void testNull() {
		
		new Field(null);
		
	}
	
	@Test
	public void testEmpty() {
		
		Field ve = new Field("");
		Assert.assertTrue(ve.isEmpty());
		
	}
	
	@Test
	public void testSingle() {
		
		Field ve = new Field("logins");
		Assert.assertFalse(ve.isEmpty());
		Assert.assertEquals(1, ve.size());
		Assert.assertEquals("logins", ve.get(0));
		
		//System.out.println(ve);
		
	}
	
	@Test
	public void testSingleIndex() {
		
		Field ve = new Field("[4]");
		Assert.assertFalse(ve.isEmpty());
		Assert.assertEquals(1, ve.size());
		Assert.assertEquals("4", ve.get(0));
		
		//System.out.println(ve);
		
	}
	
	@Test
	public void testTwo() {
		
		Field ve = new Field("name.first");
		Assert.assertFalse(ve.isEmpty());
		Assert.assertEquals(2, ve.size());
		Assert.assertEquals("name", ve.get(0));
		Assert.assertEquals("first", ve.get(1));
		
		//System.out.println(ve);
		
	}
	
	@Test
	public void testTwoWithIndex() {
		
		Field ve = new Field("name[1]");
		Assert.assertFalse(ve.isEmpty());
		Assert.assertEquals(2, ve.size());
		Assert.assertEquals("name", ve.get(0));
		Assert.assertEquals("1", ve.get(1));
		
		//System.out.println(ve);
		
	}
	
	@Test
	public void testComplex() {
		
		Field ve = new Field("educations[2].name");
		Assert.assertFalse(ve.isEmpty());
		Assert.assertEquals(3, ve.size());
		Assert.assertEquals("educations", ve.get(0));
		Assert.assertEquals("2", ve.get(1));
		Assert.assertEquals("name", ve.get(2));
		
		//System.out.println(ve);
		
	}
	
	@Test
	public void testSimple() {
		
		Table table = new Table();
		table.put("name", "John");
		
		Field ve = new Field("name");
		
		Assert.assertEquals(new Str("John"), table.get(ve));
		Assert.assertEquals(new Str("John"), table.get(ve).asStr());
		
	}
	
	@Test
	public void testInner() {
		
		Table innerTable = new Table();
		innerTable.put("first", "John");
		innerTable.put("last", "Dow");
		
		Table table = new Table();
		table.put("name", innerTable);
		
		Field ve = new Field("name.first");
		
		Assert.assertEquals(new Str("John"), table.get(ve));
		Assert.assertEquals(new Str("John"), table.get(ve).asStr());
		
	}
	
	@Test
	public void testInnerWithIndex() {
		
		Table innerTable = new Table();
		innerTable.put(1, "John");
		innerTable.put(2, "Dow");
		
		Table table = new Table();
		table.put("name", innerTable);
		
		Field ve = new Field("name[1]");
		
		Assert.assertEquals(new Str("John"), table.get(ve));
		Assert.assertEquals(new Str("John"), table.get(ve).asStr());
		
	}
	
	@Test
	public void testEmptyPut() {
		
		Table table = new Table();
		Field ve = new Field("name");

		table.put(ve, "John");
		
		Assert.assertEquals(new Str("John"), table.get(ve));
		Assert.assertEquals(new Str("John"), table.get(ve).asStr());
	}
	
	@Test
	public void testEmptyInnerPut() {
		
		Table table = new Table();
		Field ve = new Field("name.first");

		table.put(ve, "John");
		
		Assert.assertEquals(new Str("John"), table.get(ve));
		Assert.assertEquals(new Str("John"), table.get(ve).asStr());
		
	}
	
	@Test
	public void testIncrement() {
		
		Table table = new Table();
		Field ve = new Field("logins");

		Num number = table.get(ve).asNum();
		if (number == null) {
			number = new Num(0l);
		}
		number = number.add(new Num(1));
		
		table.put(ve, number);
		
		Assert.assertEquals(1, table.get(ve).asNum().asLong());
		
	}
	
	@Test
	public void testDecrement() {
		
		Table table = new Table();
		Field ve = new Field("logins");

		Num number = table.get(ve).asNum();
		if (number == null) {
			number = new Num(0l);
		}
		number = number.subtract(new Num(1.0));
		
		table.put(ve, number);
		
		Assert.assertEquals(-1, table.get(ve).asNum().asLong());
		
	}

	
}
