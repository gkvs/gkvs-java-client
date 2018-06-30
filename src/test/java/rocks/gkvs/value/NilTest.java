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
 * NilTest
 *
 * @author Alex Shvid
 * @date Jun 30, 2018 
 *
 */

public class NilTest {

	@Test
	public void testDefault() {
	
		
		Assert.assertEquals(Value.DEFAULT_BOOL, Nil.get().asBool());
		Assert.assertEquals(Value.DEFAULT_NUM, Nil.get().asNum());
		Assert.assertEquals(Value.DEFAULT_STR, Nil.get().asStr());
		Assert.assertEquals(Value.DEFAULT_TABLE, Nil.get().asTable());
		
	}
	
	@Test
	public void testNilTableGet() {
		
		Assert.assertEquals(Nil.get(), Nil.get().asTable().get("key"));
		
	}
	
	@Test(expected=UnsupportedOperationException.class)
	public void testNilTableUpdate() {
		
		Nil.get().asTable().put("key", "value");
		
	}
	
}
