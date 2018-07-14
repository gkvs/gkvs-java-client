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

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;


/**
 * 
 * KeyTest
 *
 * @author Alex Shvid
 * @date Jul 13, 2018 
 *
 */

public class KeyTest {

	private Random random = new Random();
	
	@Test
	public void testRaw() {
		
		Key key = Key.raw("test", "alex");
		
		String str = key.getRecordKeyString();
		
		Assert.assertFalse(str.startsWith(Key.DIGEST_PREFIX));

	}
	
	@Test
	public void testDigest() {
		
		byte[] digest = new byte[20];
		for (int i = 0; i != 20; ++i) {
			digest[i] = (byte) random.nextInt(256);
		}
		
		Key key = Key.digest("test", digest);
		
		String str = key.getRecordKeyString();
		
		Assert.assertTrue(str.startsWith(Key.DIGEST_PREFIX));
		
		//String str = key.toString();
		//System.out.println(str);
		
	}
	
}
