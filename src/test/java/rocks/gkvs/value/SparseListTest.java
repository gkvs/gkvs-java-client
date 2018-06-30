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

import java.util.Random;

import org.junit.Assert;
import org.junit.Test;

/**
 * 
 * SparseListTest
 *
 * @author Alex Shvid
 * @date Jun 29, 2018 
 *
 */

public class SparseListTest {

	private final static Random rand = new Random();
	
	@Test
	public void testEmpty() {
	
		SparseList<Integer> list = new SparseList<>();
		
		Assert.assertTrue(list.isEmpty());
		Assert.assertEquals(0, list.size());
		
		
		//System.out.println(list);
	}
	
	@Test
	public void testOne() {
		
		SparseList<Integer> list = new SparseList<>();
		
		list.add(100);
		
		Assert.assertFalse(list.isEmpty());
		Assert.assertEquals(1, list.size());
		
		Integer actual = list.get(SparseList.BASE);
		Assert.assertNotNull(actual);
		Assert.assertEquals(100, actual.intValue());
		
		actual = list.get(list.keyAt(0));
		Assert.assertNotNull(actual);
		Assert.assertEquals(100, actual.intValue());
		
		int idx = list.indexOf(100);
		actual = list.valueAt(idx);
		
		Assert.assertEquals(100, actual.intValue());
		
		//System.out.println(list);
	}
	
	@Test
	public void testStr() {
		
		SparseList<String> list = new SparseList<>();
		
		list.add(100, "alex");
		
		//System.out.println(list);
		
		Assert.assertFalse(list.isEmpty());
		Assert.assertEquals(1, list.size());
		
		String actual = list.get(100);
		Assert.assertNotNull(actual);
		Assert.assertEquals("alex", actual);
		
		int idx = list.indexOf("alex");
		actual = list.valueAt(idx);
		
		Assert.assertEquals("alex", actual);
		
	}
	
	@Test
	public void testSeqStr() {
		
		SparseList<String> list = new SparseList<>();
		
		list.add("alex");
		list.add(list.lastKey()+1, "shvid");
		
		//System.out.println(list);
		
		Assert.assertFalse(list.isEmpty());
		Assert.assertEquals(2, list.size());
		
		String actual = list.get(SparseList.BASE);
		Assert.assertNotNull(actual);
		Assert.assertEquals("alex", actual);
		
		int idx = list.indexOf("alex");
		actual = list.valueAt(idx);
		
		Assert.assertEquals("alex", actual);
		
	}
	
	@Test
	public void testInsert() {
		
		SparseList<String> list = new SparseList<>();
		
		list.add("shvid");
		list.add(0, "alex");
		
		//System.out.println(list);
		
		Assert.assertFalse(list.isEmpty());
		Assert.assertEquals(2, list.size());
		
		String actual = list.get(0);
		Assert.assertNotNull(actual);
		Assert.assertEquals("alex", actual);
		
		int idx = list.indexOf("alex");
		actual = list.valueAt(idx);
		
		Assert.assertEquals("alex", actual);
		
	}
	
	@Test
	public void testOverflow() {
		
		SparseList<Integer> list = new SparseList<>();
		
		Assert.assertEquals(0, list.firstKey());
		Assert.assertEquals(SparseList.BASE-1, list.lastKey());
		
		for (int i = 100; i != 200; ++i) {
			list.set(i, i);
		}
		
		//System.out.println(list);
		
		Assert.assertFalse(list.isEmpty());
		Assert.assertEquals(100, list.size());
		
		for (int index = 0; index != list.size(); ++index) {
			
			Assert.assertEquals(list.keyAt(index), list.valueAt(index).intValue());
			
		}
		
		Assert.assertEquals(100, list.firstKey());
		Assert.assertEquals(199, list.lastKey());
		
		Assert.assertFalse(list.isSequence());
		Assert.assertTrue(list.isSequence(100));
		
	}
	
	
	@Test
	public void testRandom() {
	
		SparseList<Integer> list = new SparseList<>();
		
		for (int i = 0; i != 100; ++i) {
			
			int idx = rand.nextInt(10000);
			
			list.set(idx, idx);
		}
		
		Assert.assertFalse(list.isEmpty());
		Assert.assertTrue(100 >= list.size());
		
		for (int index = 0; index != list.size(); ++index) {
			
			Assert.assertEquals(list.keyAt(index), list.valueAt(index).intValue());
			
		}
	}
	
	
	@Test
	public void testRemove() {
		
		SparseList<Integer> list = new SparseList<>();
		
		list.add(123);
		
		Assert.assertFalse(list.isEmpty());
		Assert.assertEquals(1, list.size());
		Assert.assertTrue(list.isSequence());
		
		// by val
		list.remove(Integer.valueOf(123));

		Assert.assertTrue(list.isEmpty());
		Assert.assertEquals(0, list.size());
		
		/**
		 * NEXT TEST
		 */
		
		list.add(555);
		
		Assert.assertFalse(list.isEmpty());
		Assert.assertEquals(1, list.size());
		Assert.assertTrue(list.isSequence());
		
		// by key
		list.remove(list.firstKey());
		
		Assert.assertTrue(list.isEmpty());
		Assert.assertEquals(0, list.size());
	}
	
	
}
