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
 * BoolTest
 *
 * @author Alex Shvid
 * @date Jun 30, 2018 
 *
 */

public class BoolTest extends AbstractValueTest {

	@Test
	public void testNull() {

		Bool bool = new Bool(null);

		Assert.assertEquals(false, bool.asBoolean());
		Assert.assertEquals("c2", toHexString(bool.toMsgpackValue()));
		Assert.assertEquals("c2", toHexString(bool));

		Bool actual = Parser.parseTypedValue(bool.toMsgpack());
		Assert.assertEquals(bool, actual);
	}

	@Test
	public void testEmpty() {

		Bool bool = new Bool("");

		Assert.assertEquals(false, bool.asBoolean());
		Assert.assertEquals("c2", toHexString(bool.toMsgpackValue()));
		Assert.assertEquals("c2", toHexString(bool));

		Bool actual = Parser.parseTypedValue(bool.toMsgpack());
		Assert.assertEquals(bool, actual);
	}

	@Test
	public void testFalse() {

		Bool bool = new Bool("false");

		Assert.assertEquals(false, bool.asBoolean());
		Assert.assertEquals("c2", toHexString(bool.toMsgpackValue()));
		Assert.assertEquals("c2", toHexString(bool));

		Bool actual = Parser.parseTypedValue(bool.toMsgpack());
		Assert.assertEquals(bool, actual);
	}

	@Test
	public void testUnknown() {

		Bool bool = new Bool("unknown");

		Assert.assertEquals(false, bool.asBoolean());
		Assert.assertEquals("c2", toHexString(bool.toMsgpackValue()));
		Assert.assertEquals("c2", toHexString(bool));

		Bool actual = Parser.parseTypedValue(bool.toMsgpack());
		Assert.assertEquals(bool, actual);
	}

	@Test
	public void testTrue() {

		Bool bool = new Bool("true");

		Assert.assertEquals(true, bool.asBoolean());
		Assert.assertEquals("c3", toHexString(bool.toMsgpackValue()));
		Assert.assertEquals("c3", toHexString(bool));

		Bool actual = Parser.parseTypedValue(bool.toMsgpack());
		Assert.assertEquals(bool, actual);
	}

}
