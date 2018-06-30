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

import java.util.Arrays;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * StrTest
 *
 * @author Alex Shvid
 * @date Jun 30, 2018
 *
 */

public class StrTest extends AbstractValueTest {

	@Test(expected = IllegalArgumentException.class)
	public void testNullString() {

		new Str((String) null);

	}

	@Test(expected = IllegalArgumentException.class)
	public void testNullBytes() {

		new Str((byte[]) null, false);

	}

	@Test
	public void testEmptyString() {

		Str string = new Str("");

		Assert.assertEquals("", string.asString());
		Assert.assertEquals("", string.asUtf8());
		Assert.assertTrue(Arrays.equals(new byte[0], string.asBytes()));

		Assert.assertEquals("a0", toHexString(string.toMsgpackValue()));
		Assert.assertEquals("a0", toHexString(string));

		Str actual = Parser.parseTypedValue(string.toMsgpack());
		Assert.assertEquals(string, actual);
	}

	@Test
	public void testString() {

		Str string = new Str("hello");

		Assert.assertEquals("hello", string.asString());
		Assert.assertEquals("hello", string.asUtf8());
		Assert.assertTrue(Arrays.equals("hello".getBytes(), string.asBytes()));

		Assert.assertEquals("a568656c6c6f", toHexString(string.toMsgpackValue()));
		Assert.assertEquals("a568656c6c6f", toHexString(string));

		Str actual = Parser.parseTypedValue(string.toMsgpack());
		Assert.assertEquals(string, actual);

	}

}
