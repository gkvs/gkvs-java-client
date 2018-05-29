/*
 *
 * Copyright 2018 gKVS authors.
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

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.Iterator;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import rocks.gkvs.protos.GenericStoreGrpc;
import rocks.gkvs.protos.GenericStoreGrpc.GenericStoreBlockingStub;
import rocks.gkvs.protos.GenericStoreGrpc.GenericStoreFutureStub;
import rocks.gkvs.protos.GenericStoreGrpc.GenericStoreStub;

public final class GkvsClient implements Closeable {

	private static final boolean NO_SINGLTON = Boolean.getBoolean("gkvs.no_singleton"); 
	private static volatile GkvsClient defaultInstance = null;
	
	private final ManagedChannel channel;
	private final GenericStoreBlockingStub blockingStub;
	private final GenericStoreStub asyncStub;
	private final GenericStoreFutureStub futureStub;
	
    private final Cache<Long, Key> waitingQueue = CacheBuilder.newBuilder()
    		.expireAfterWrite(20, TimeUnit.MINUTES)
    		.concurrencyLevel(16)
    		.build();
    
	private final AtomicLong sequenceNum = new AtomicLong(1L);
	
	public static GkvsClient createFromClasspath() {
		return createFromClasspath(GkvsClient.class.getClassLoader());
	}
	
	public static GkvsClient createFromClasspath(ClassLoader classLoader) {
		
		InputStream in = classLoader.getResourceAsStream("gkvs-override.properties");
		if (in == null) {
			in = classLoader.getResourceAsStream("gkvs-default.properties");
			if (in == null) {
				throw new IllegalStateException("expected gkvs-override.properties or gkvs-default.properties in classpath");
			}
		}
		
		Properties props = new Properties();

		try {
			props.load(in);
		} catch (IOException e) {
			throw new IllegalStateException("unable to load properties");
		}
		finally {
			try {
				in.close();
			} catch (IOException e) {
			}
		}

		return createFromProperties(props);
	}
	
	public static GkvsClient createFromProperties(Properties props) {
		String host = props.getProperty("gkvs.host", "localhost");
		int port;
		try {
			port = Integer.parseInt(props.getProperty("gkvs.port", "4040"));
		}
		catch(NumberFormatException e) {
			throw new IllegalStateException("unable parse gkvs.port property", e);
		}
		return new GkvsClient(host, port);
	}
	
	public GkvsClient(String host, int port) {
		this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
	}
	
	public GkvsClient(ManagedChannelBuilder<?> channelBuilder) {
		channel = channelBuilder.build();
		blockingStub = GenericStoreGrpc.newBlockingStub(channel);
		asyncStub = GenericStoreGrpc.newStub(channel);
		futureStub = GenericStoreGrpc.newFutureStub(channel);
	}
	
	public static GkvsClient getDefaultInstance() {
		if (NO_SINGLTON) {
			throw new IllegalStateException("gKVS singleton is not allowed");
		}
		if (defaultInstance == null) {
			synchronized (Gkvs.class) {
				if (defaultInstance == null) {
					
					defaultInstance = GkvsClient.createFromClasspath();
					
				     Runtime.getRuntime().addShutdownHook(new Thread() {
				    	 @Override
				           public void run() {
				    		 try {
								defaultInstance.close();
							} catch (IOException e) {
								e.printStackTrace();
							}
				            }
				        });
				}
			}
		}
		return defaultInstance;
	}
	
	protected GenericStoreBlockingStub getBlockingStub() {
		return blockingStub;
	}

	protected GenericStoreStub getAsyncStub() {
		return asyncStub;
	}

	protected GenericStoreFutureStub getFutureStub() {
		return futureStub;
	}

	protected long nextRequestId() {
		long num = sequenceNum.incrementAndGet();
		if (num > Long.MAX_VALUE - 100) {
			if (!sequenceNum.compareAndSet(num, 1)) {
				return nextRequestId();
			}
			return 1L;
		}
	    return num;
	}
	
	protected void pushWaitingQueue(long requestId, Key key) {
		waitingQueue.put(requestId, key);
	}
	
	protected @Nullable Key popWaitingQueue(long requestId) {
		Key key =  waitingQueue.getIfPresent(requestId);
		if (key != null) {
			waitingQueue.invalidate(requestId);
		}
		return key;
	}
		
	@Override
	public void close() throws IOException {
		try {
			channel.shutdown().awaitTermination(5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			throw new IOException(e);
		}
	}
	
	public Get exists(String tableName, String recordKey) {
		return new Get(this).setKey(Key.raw(tableName, recordKey)).metadataOnly();
	}
	
	public Get exists(Key key) {
		return new Get(this).setKey(key).metadataOnly();
	}
	
	public Get get(String tableName, String recordKey) {
		return new Get(this).setKey(Key.raw(tableName, recordKey));
	}
	
	public Get get(Key key) {
		return new Get(this).setKey(key);
	}
	
	public MultiGet multiGet(Key...keys) {
		return new MultiGet(this).setKeys(keys);
	}
	
	public MultiGet multiGet(Iterator<Key> keys) {
		return new MultiGet(this).setKeys(keys);
	}
	
	public MultiGet multiGet(Iterable<Key> keys) {
		return new MultiGet(this).setKeys(keys);
	}

	public GetAll getAll() {
		return new GetAll(this);
	}
	
	public Put putWithKey(String tableName, String recordKey) {
		return new Put(this)
				.setKey(Key.raw(tableName, recordKey));
	}
	
	public Put putWithKey(Key key) {
		return new Put(this).setKey(key);
	}
	
	public Put put(String tableName, String recordKey, String value) {
		return putWithKey(tableName, recordKey).put(Value.of(value));
	}
	
	public Put put(Key key, String value) {
		return putWithKey(key).put(Value.of(value));
	}
	
	public Put put(String tableName, String recordKey, String column, String value) {
		return putWithKey(tableName, recordKey).put(Value.of(column, value));
	}
	
	public Put put(Key key, String column, String value) {
		return putWithKey(key).put(Value.of(column, value));
	}

	public Put put(String tableName, String recordKey, byte[] value) {
		return putWithKey(tableName, recordKey).put(Value.of(value));
	}
	
	public Put put(Key key, byte[] value) {
		return putWithKey(key).put(Value.of(value));
	}
	
	public Put put(String tableName, String recordKey, String column, byte[] value) {
		return putWithKey(tableName, recordKey).put(Value.of(column, value));
	}
	
	public Put put(Key key, String column, byte[] value) {
		return putWithKey(key).put(Value.of(column, value));
	}

	public Put put(Key key, Value value) {
		return putWithKey(key).put(value);
	}

	public Put put(Key key, Value... values) {
		return putWithKey(key).putAll(values);
	}

	public Put put(Key key, Iterable<Value> values) {
		return putWithKey(key).putAll(values);
	}

	public Put put(Key key, Map<String, byte[]> values) {
		return putWithKey(key).putAll(values);
	}
	
	public PutAll putAll() {
		return new PutAll(this);
	}
	
	public Remove remove(String tableName, String recordKey) {
		return new Remove(this).setKey(Key.raw(tableName, recordKey));
	}
	
	public Remove remove(Key key) {
		return new Remove(this).setKey(key);
	}
	
	public RemoveAll removeAll() {
		return new RemoveAll(this);
	}
	
	public Scan scan(String tableName) {
		return new Scan(this).table(tableName);
	}
	
}
