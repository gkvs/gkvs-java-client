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
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import rocks.gkvs.protos.GenericStoreGrpc;
import rocks.gkvs.protos.GenericStoreGrpc.GenericStoreBlockingStub;
import rocks.gkvs.protos.GenericStoreGrpc.GenericStoreStub;
import rocks.gkvs.protos.Status;
import rocks.gkvs.protos.StatusCode;

public final class GKVSClient implements Closeable {

	private static final boolean NO_SINGLTON = Boolean.getBoolean("gkvs.no_singleton"); 
	private static volatile GKVSClient defaultInstance = null;
	
	private final ManagedChannel channel;
	private final GenericStoreBlockingStub blockingStub;
	private final GenericStoreStub asyncStub;
	
	private final AtomicLong sequenceNum = new AtomicLong(1L);
	
	public GKVSClient() {
		this("localhost", 4040);
	}
	
	public static GKVSClient createFromClasspath() {
		return new GKVSClient(); 
	}
	
	public GKVSClient(String host, int port) {
		this(ManagedChannelBuilder.forAddress(host, port).usePlaintext());
	}
	
	public GKVSClient(ManagedChannelBuilder<?> channelBuilder) {
		channel = channelBuilder.build();
		blockingStub = GenericStoreGrpc.newBlockingStub(channel);
		asyncStub = GenericStoreGrpc.newStub(channel);
	}
	
	public static GKVSClient getDefaultInstance() {
		if (NO_SINGLTON) {
			throw new IllegalStateException("gKVS singleton is not allowed");
		}
		if (defaultInstance == null) {
			synchronized (GKVS.class) {
				if (defaultInstance == null) {
					defaultInstance = GKVSClient.createFromClasspath();
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
	
	protected long nextSequenceNum() {
		long num = sequenceNum.incrementAndGet();
		if (num > Long.MAX_VALUE - 100) {
			if (!sequenceNum.compareAndSet(num, 1)) {
				return nextSequenceNum();
			}
			return 1L;
		}
	    return num;
	}
	
	protected void postProcess(Status status) {
		if (!success(status.getCode())) {
			throw new GKVSResultException(status);
		}
	}

	protected static boolean success(StatusCode code) {
		switch(code) {
		case SUCCESS:
		case SUCCESS_NOT_UPDATED:
		case SUCCESS_END_STREAM:
			return true;
		default:
			return false;
		}
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
	
	public Put put(String tableName, String recordKey, String column, String value) {
		return putWithKey(tableName, recordKey).put(Value.of(column, value));
	}

	public Put put(String tableName, String recordKey, byte[] value) {
		return putWithKey(tableName, recordKey).put(Value.of(value));
	}
	
	public Put put(String tableName, String recordKey, String column, byte[] value) {
		return putWithKey(tableName, recordKey).put(Value.of(column, value));
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
	
	public Remove remove(String tableName, String recordKey) {
		return new Remove(this).setKey(Key.raw(tableName, recordKey));
	}
	
	public Remove remove(Key key) {
		return new Remove(this).setKey(key);
	}
	
	public Scan scan(String tableName) {
		return new Scan(this).table(tableName);
	}
	
}
