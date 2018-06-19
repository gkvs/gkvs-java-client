package rocks.gkvs;

import java.util.UUID;

import org.junit.Test;

public class PerformanceTest extends AbstractClientTest {

	private boolean enabled = Boolean.getBoolean("perfTests");
	
	@Test
	public void runTests() {
		
		if (enabled) {
			runPerformanceCUID();
		}
	}
	

	protected void runPerformanceCUID() {
	
		long t0 = System.currentTimeMillis();
		
		for (int i = 0; i != 10000; ++i) {
			String key = UUID.randomUUID().toString();
			Gkvs.Client.put(TABLE, key, "value").sync();
			Gkvs.Client.get(TABLE, key).sync().value();
			Gkvs.Client.remove(TABLE, key);
		}
		
		long diff = System.currentTimeMillis() - t0;
		
		System.out.println("30000 requests in " + diff + " milliseconds, " + (double) diff / 30000.0);
	}
	
}
