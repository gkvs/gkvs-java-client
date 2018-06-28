package rocks.gkvs;

import java.util.UUID;

import org.junit.Test;

import rocks.gkvs.value.Str;

/**
 * 
 * PerformanceTest
 * 
 * To enable performance tests build with -DperfTests=true param
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

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
			Gkvs.Client.put(STORE, key, new Str("value")).sync();
			Gkvs.Client.get(STORE, key).sync().value();
			Gkvs.Client.remove(STORE, key);
		}
		
		long diff = System.currentTimeMillis() - t0;
		
		System.out.println("30000 requests in " + diff + " milliseconds, " + (double) diff / 30000.0);
	}
	
}
