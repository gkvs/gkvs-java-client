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
 * NumDetectorTest
 *
 * @author Alex Shvid
 * @date Jun 29, 2018 
 *
 */

public class NumDetectorTest {

	@Test
	public void testNull() {
		
		Assert.assertNull(Num.detectNumber(null));
		
	}
	
	@Test
	public void testEmpty() {
		
		Assert.assertNull(Num.detectNumber(""));
		
	}
	
	@Test
	public void testINT64() {
		
		Assert.assertEquals(NumType.INT64, Num.detectNumber("123456789"));
		
	}
	
	@Test
	public void testMinusINT64() {
		
		Assert.assertEquals(NumType.INT64, Num.detectNumber("-123456789"));
		
	}
	
	@Test
	public void testInvalidMinusINT64() {
		
		Assert.assertNull(Num.detectNumber("123-456789"));
		
	}
	
	@Test
	public void testFloat64() {
		
		Assert.assertEquals(NumType.FLOAT64, Num.detectNumber("123456789.0"));
		
	}
	
	@Test
	public void testFloat64WithE() {
		
		Assert.assertEquals(NumType.FLOAT64, Num.detectNumber("1.23456789E8"));
		
	}
	
	@Test
	public void testFloat64WithInvalidE() {
		
		Assert.assertNull(Num.detectNumber("1.2345E789E8"));
		
	}
	
	@Test
	public void testInvalidDouble() {
		
		Assert.assertNull(Num.detectNumber("12345.6789.0"));
		
	}
	
	@Test
	public void testMinusDouble() {
		
		Assert.assertEquals(NumType.FLOAT64, Num.detectNumber("-123456789.0"));
		
	}
	
	@Test
	public void testMinusDoubleE() {
		
		Assert.assertEquals(NumType.FLOAT64, Num.detectNumber("-1.23456789E8"));
		
	}
	
}
