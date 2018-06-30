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
 * NumTest
 *
 * @author Alex Shvid
 * @date Jun 30, 2018 
 *
 */

public class NumTest extends AbstractValueTest {

	@Test(expected = IllegalArgumentException.class)
	public void testNull() {

		new Num(null);

	}

	@Test(expected = IllegalArgumentException.class)
	public void testEmpty() {

		new Num("");

	}

	@Test(expected = IllegalArgumentException.class)
	public void testNAN() {

		new Num("abc");

	}

	@Test
	public void testZeroLong() {

		Num number = new Num("0");

		Assert.assertEquals(NumType.INT64, number.getType());
		Assert.assertEquals(0L, number.asLong());
		Assert.assertTrue(Math.abs(0.0 - number.asDouble()) < 0.001);
		Assert.assertEquals("0", number.asString());

		Assert.assertEquals("00", toHexString(number.toMsgpackValue()));
		Assert.assertEquals("00", toHexString(number));

		Num actual = Parser.parseTypedValue(number.toMsgpack());
		Assert.assertEquals(number, actual);
	}

	@Test
	public void testZeroDouble() {

		Num number = new Num("0.0");

		Assert.assertEquals(NumType.FLOAT64, number.getType());
		Assert.assertEquals(0L, number.asLong());
		Assert.assertTrue(Math.abs(0.0 - number.asDouble()) < 0.001);
		Assert.assertEquals("0.0", number.asString());

		Assert.assertEquals("cb0000000000000000", toHexString(number.toMsgpackValue()));
		Assert.assertEquals("cb0000000000000000", toHexString(number));

		Num actual = Parser.parseTypedValue(number.toMsgpack());
		Assert.assertEquals(number, actual);
	}

	@Test
	public void testLong() {

		Num number = new Num("123456789");

		Assert.assertEquals(NumType.INT64, number.getType());
		Assert.assertEquals(123456789L, number.asLong());
		Assert.assertTrue(Math.abs(123456789.0 - number.asDouble()) < 0.001);
		Assert.assertEquals("123456789", number.asString());

		Assert.assertEquals("ce075bcd15", toHexString(number.toMsgpackValue()));
		Assert.assertEquals("ce075bcd15", toHexString(number));

		Num actual = Parser.parseTypedValue(number.toMsgpack());
		Assert.assertEquals(number, actual);
	}

	@Test
	public void testDouble() {

		Num number = new Num("123456789.0");

		Assert.assertEquals(NumType.FLOAT64, number.getType());
		Assert.assertEquals(123456789L, number.asLong());
		Assert.assertTrue(Math.abs(123456789.0 - number.asDouble()) < 0.001);
		Assert.assertEquals("1.23456789E8", number.asString());

		Assert.assertEquals("cb419d6f3454000000", toHexString(number.toMsgpackValue()));
		Assert.assertEquals("cb419d6f3454000000", toHexString(number));

		Num actual = Parser.parseTypedValue(number.toMsgpack());
		Assert.assertEquals(number, actual);
	}

	@Test
	public void testMinusLong() {

		Num number = new Num("-123456789");

		Assert.assertEquals(NumType.INT64, number.getType());
		Assert.assertEquals(-123456789L, number.asLong());
		Assert.assertTrue(Math.abs(-123456789.0 - number.asDouble()) < 0.001);
		Assert.assertEquals("-123456789", number.asString());

		Assert.assertEquals("d2f8a432eb", toHexString(number.toMsgpackValue()));
		Assert.assertEquals("d2f8a432eb", toHexString(number));

		Num actual = Parser.parseTypedValue(number.toMsgpack());
		Assert.assertEquals(number, actual);
	}

	@Test
	public void testMinusDouble() {

		Num number = new Num("-123456789.0");

		Assert.assertEquals(NumType.FLOAT64, number.getType());
		Assert.assertEquals(-123456789L, number.asLong());
		Assert.assertTrue(Math.abs(-123456789.0 - number.asDouble()) < 0.001);
		Assert.assertEquals("-1.23456789E8", number.asString());

		Assert.assertEquals("cbc19d6f3454000000", toHexString(number.toMsgpackValue()));
		Assert.assertEquals("cbc19d6f3454000000", toHexString(number));

		Num actual = Parser.parseTypedValue(number.toMsgpack());
		Assert.assertEquals(number, actual);
	}

	@Test
	public void testMaxLong() {

		Num number = new Num(Long.MAX_VALUE);

		Assert.assertEquals(NumType.INT64, number.getType());
		Assert.assertEquals(Long.MAX_VALUE, number.asLong());
		Assert.assertTrue(Math.abs((double) Long.MAX_VALUE - number.asDouble()) < 0.001);
		Assert.assertEquals(Long.toString(Long.MAX_VALUE), number.asString());

		Assert.assertEquals("cf7fffffffffffffff", toHexString(number.toMsgpackValue()));
		Assert.assertEquals("cf7fffffffffffffff", toHexString(number));

		Num actual = Parser.parseTypedValue(number.toMsgpack());
		Assert.assertEquals(number, actual);
	}

	@Test
	public void testMinLong() {

		Num number = new Num(Long.MIN_VALUE);

		Assert.assertEquals(NumType.INT64, number.getType());
		Assert.assertEquals(Long.MIN_VALUE, number.asLong());
		Assert.assertTrue(Math.abs((double) Long.MIN_VALUE - number.asDouble()) < 0.001);
		Assert.assertEquals(Long.toString(Long.MIN_VALUE), number.asString());

		Assert.assertEquals("d38000000000000000", toHexString(number.toMsgpackValue()));
		Assert.assertEquals("d38000000000000000", toHexString(number));

		Num actual = Parser.parseTypedValue(number.toMsgpack());
		Assert.assertEquals(number, actual);
	}

	@Test
	public void testAdd() {

		Num number = new Num(123L);

		Assert.assertEquals(123L + 555L, number.add(555L).asLong());
		Assert.assertEquals(123L + 555L, number.add(new Num(555L)).asLong());
		Assert.assertEquals(126L, number.add(3.0).asLong());
		Assert.assertEquals(126L, number.add(new Num(3.0)).asLong());

		number = new Num(123.0);

		Assert.assertEquals(123L + 555L, number.add(555L).asLong());
		Assert.assertEquals(123L + 555L, number.add(new Num(555L)).asLong());
		Assert.assertEquals(126L, number.add(3.0).asLong());
		Assert.assertEquals(126L, number.add(new Num(3.0)).asLong());
	}

	@Test
	public void testSubtruct() {

		Num number = new Num(123L);

		Assert.assertEquals(123L - 555L, number.subtract(555L).asLong());
		Assert.assertEquals(123L - 555L, number.subtract(new Num(555L)).asLong());
		Assert.assertEquals(120L, number.subtract(3.0).asLong());
		Assert.assertEquals(120L, number.subtract(new Num(3.0)).asLong());

		number = new Num(123.0);

		Assert.assertEquals(123L - 555L, number.subtract(555L).asLong());
		Assert.assertEquals(123L - 555L, number.subtract(new Num(555L)).asLong());
		Assert.assertEquals(120L, number.subtract(3.0).asLong());
		Assert.assertEquals(120L, number.subtract(new Num(3.0)).asLong());
	}
}
