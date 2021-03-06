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

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import javax.annotation.Nullable;
import javax.net.ssl.SSLException;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.netty.GrpcSslContexts;
import io.grpc.netty.NegotiationType;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.SupportedCipherSuiteFilter;
import rocks.gkvs.protos.GenericStoreGrpc;
import rocks.gkvs.protos.GenericStoreGrpc.GenericStoreBlockingStub;
import rocks.gkvs.protos.GenericStoreGrpc.GenericStoreFutureStub;
import rocks.gkvs.protos.GenericStoreGrpc.GenericStoreStub;
import rocks.gkvs.value.Value;

/**
 * 
 * GkvsClient
 *
 * Instance of the GKVS
 *
 * @author Alex Shvid
 * @date Jun 18, 2018 
 *
 */

public final class GkvsClient implements Closeable {
	
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
		GkvsConfig config = GkvsConfig.fromProperties(props);
		return new GkvsClient(config);
	}
	
	public GkvsClient(GkvsConfig config) {
		this(buildChannel(config));
	}
	
	public GkvsClient(ManagedChannelBuilder<?> channelBuilder) {
		channel = channelBuilder.build();
		blockingStub = GenericStoreGrpc.newBlockingStub(channel);
		asyncStub = GenericStoreGrpc.newStub(channel);
		futureStub = GenericStoreGrpc.newFutureStub(channel);
	}
	
	public static GkvsClient getDefaultInstance() {
		if (!GkvsConstants.USE_SINGLTON) {
			throw new IllegalStateException("GKVS singleton is not allowed");
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

	protected long nextTag() {
		long num = sequenceNum.incrementAndGet();
		if (num > Long.MAX_VALUE - 100) {
			if (!sequenceNum.compareAndSet(num, 1)) {
				return nextTag();
			}
			return 1L;
		}
	    return num;
	}
	
	protected void pushWaitingQueue(long tag, Key key) {
		waitingQueue.put(tag, key);
	}
	
	protected @Nullable Key popWaitingQueue(long tag) {
		Key key =  waitingQueue.getIfPresent(tag);
		if (key != null) {
			waitingQueue.invalidate(tag);
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
	
	public ListCmd list() {
		return new ListCmd(this);
	}
	
	public Get exists(String viewName, String recordKey) {
		return new Get(this).setKey(Key.raw(viewName, recordKey)).metadataOnly();
	}
	
	public Get exists(Key key) {
		return new Get(this).setKey(key).metadataOnly();
	}
	
	public Get get(String viewName, String recordKey) {
		return new Get(this).setKey(Key.raw(viewName, recordKey));
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
	
	public Put put(String viewName, String recordKey, Value value) {
		return new Put(this).put(Key.raw(viewName, recordKey), value);
	}
	
	public Put put(Key key, Value value) {
		return new Put(this).put(key, value);
	}
	
	public Put put(KeyValue keyValue) {
		return new Put(this).put(keyValue);
	}
	
	public Put putIfAbsent(String viewName, String recordKey, Value value) {
		return new Put(this).putIfAbsent(Key.raw(viewName, recordKey), value);
	}
	
	public Put putIfAbsent(Key key, Value value) {
		return new Put(this).putIfAbsent(key, value);
	}
	
	public Put putIfAbsent(KeyValue keyValue) {
		return new Put(this).putIfAbsent(keyValue);
	}
	
	public Put compareAndPut(String viewName, String recordKey, Value value, @Nullable int[] version) {
		return new Put(this).compareAndPut(Key.raw(viewName, recordKey), value, version);
	}
	
	public Put compareAndPut(Key key, Value value, @Nullable int[] version) {
		return new Put(this).compareAndPut(key, value, version);
	}
	
	public Put compareAndPut(KeyValue keyValue, @Nullable int[] version) {
		return new Put(this).compareAndPut(keyValue, version);
	}
	
	public PutAll putAll() {
		return new PutAll(this);
	}
	
	public Remove remove(String viewName, String recordKey) {
		return new Remove(this).setKey(Key.raw(viewName, recordKey));
	}
	
	public Remove remove(Key key) {
		return new Remove(this).setKey(key);
	}
	
	public RemoveAll removeAll() {
		return new RemoveAll(this);
	}
	
	public Scan scan(String viewName) {
		return new Scan(this).view(viewName);
	}
	
	private static File getCertAuthFile(String gkvsKeys) {
		
		if (gkvsKeys.startsWith(GkvsConstants.CLASSPATH_PREFIX)) {
			
			ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
			URL url = classLoader.getResource(gkvsKeys.substring(GkvsConstants.CLASSPATH_PREFIX_LENGTH) + GkvsConstants.GKVS_AUTH_CRT);
			if (url == null) {
				throw new IllegalArgumentException("GkvsAuth.crt resource not found in " + gkvsKeys);
			}
			
			return new File(url.getPath());
		}
		else {
			return new File(gkvsKeys + File.separator + GkvsConstants.GKVS_AUTH_CRT);
		}

	}
	
	private static NettyChannelBuilder buildChannel(GkvsConfig config) {

		if (config.useSsl()) {
		
			return NettyChannelBuilder.forAddress(config.getHost(), config.getPort()).negotiationType(NegotiationType.TLS)
				.sslContext(buildSslContext(config.getSslKeys()));
		
		}
		else {
			
			return NettyChannelBuilder.forAddress(config.getHost(), config.getPort()).usePlaintext();
			
		}

	}
	
	private static SslContext buildSslContext(String gkvsKeys) {

		SslContextBuilder builder = GrpcSslContexts.forClient();
		builder.trustManager(getCertAuthFile(gkvsKeys));
		
		builder.ciphers(SslUtils.preferredCiphers(), SupportedCipherSuiteFilter.INSTANCE);
		builder.sslProvider(SslUtils.getSslProvider(GkvsConstants.SSL_PROVIDER));
		 
		try {
			return builder.build();
		} catch (SSLException e) {
			throw new GkvsException("ssl init fail", e);
		}
		
	}
	
}
