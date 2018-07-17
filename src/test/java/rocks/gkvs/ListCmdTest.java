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

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.junit.Assert;
import org.junit.Test;


/**
 * 
 * ListCmdTest
 *
 * @author Alex Shvid
 * @date Jul 16, 2018 
 *
 */

public class ListCmdTest extends AbstractClientTest {

	@Test
	public void testViews() {
		
		List<Entry> list = Gkvs.Client.list().views().sync();
		
		Set<String> names = new HashSet<String>();
		
		for (Entry entry : list) {
			names.add(entry.getKey());
		}

		Assert.assertTrue(names.contains(TEST));
		
	}
	
}
