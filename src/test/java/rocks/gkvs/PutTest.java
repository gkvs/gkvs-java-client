package rocks.gkvs;

import java.util.UUID;

import org.junit.Assert;
import org.junit.Test;

public class PutTest extends AbstractClientTest {

	@Test
	public void testPutGetRemove() {
		
		String key = UUID.randomUUID().toString();
		String value = "org";
		
		GKVS.Client.put(TABLE, key, value).sync();
		
		String actual = GKVS.Client.get(TABLE, key).sync().valueAsString();

		Assert.assertEquals(value, actual);
		
		GKVS.Client.remove(TABLE, key);
		
	}
	
}
