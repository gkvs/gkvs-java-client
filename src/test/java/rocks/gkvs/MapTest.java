package rocks.gkvs;

import java.util.Arrays;
import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

import rocks.gkvs.value.Str;
import rocks.gkvs.value.Table;

/**
 * 
 * MapTest
 *
 * @author Alex Shvid
 * @date Jun 28, 2018 
 *
 */

public class MapTest extends AbstractClientTest {

	
	@Test
	public void testMap() {
		
		String key = UUID.randomUUID().toString();
		
		Table acc = new Table();
		acc.put("accId", "1");
		acc.put("accNum", "555-555");
		
		Table tbl = new Table();
		tbl.put("account", acc);
		
		boolean updated = Gkvs.Client.put(TEST, key, tbl).sync().updated();
		
		Record rec = Gkvs.Client.get(TEST, key).sync();
		
		Table actual = rec.value().asTable();
		
		//System.out.println("actual = " + actual);
		
		Assert.assertTrue(updated);
		
		Table aa = actual.getTable("account");
		
		Assert.assertNotNull(aa);
		Assert.assertEquals(acc.getStr("accId").asString(), aa.getStr("accId").asString());
		Assert.assertEquals(acc.getStr("accNum").asString(), aa.getStr("accNum").asString());
		
		Gkvs.Client.remove(TEST, key).sync();
	}
	
	@Test
	public void testTwoMaps() {
		
		String key = UUID.randomUUID().toString();
		
		Table alex = new Table();
		alex.put("name", "alex");
		
		Table acc = new Table();
		acc.put("accId", "1");
		acc.put("accNum", "555-555");
		acc.put("person", alex);
		
		Table tbl = new Table();
		tbl.put("account", acc);
		
		boolean updated = Gkvs.Client.put(TEST, key, tbl).sync().updated();
		
		Record rec = Gkvs.Client.get(TEST, key).sync();
		
		Table actual = rec.value().asTable();
		
		//System.out.println("actual = " + actual);
		
		Assert.assertTrue(updated);
		
		Table aa = actual.getTable("account");
		
		Assert.assertNotNull(aa);
		Assert.assertEquals(acc.getStr("accId").asString(), aa.getStr("accId").asString());
		Assert.assertEquals(acc.getStr("accNum").asString(), aa.getStr("accNum").asString());
		
		Table p = aa.getTable("person");
		
		Assert.assertNotNull(p);
		Assert.assertEquals(alex.getStr("name").asString(), p.getStr("name").asString());
				
		Gkvs.Client.remove(TEST, key).sync();
		
	}
	
}
