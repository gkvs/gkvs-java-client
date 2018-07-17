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

import java.util.List;

import com.google.common.util.concurrent.ListenableFuture;

import rocks.gkvs.protos.ListOperation;
import rocks.gkvs.protos.ListResult;
import rocks.gkvs.protos.ListType;

/**
 * 
 * ListCmd
 *
 * @author Alex Shvid
 * @date Jul 16, 2018 
 *
 */

public final class ListCmd {

	private final GkvsClient instance;
	
	private ListType type;
	private String path;
	
	public ListCmd(GkvsClient instance) {
		this.instance = instance;
	}
	
	public ListType getType() {
		return type;
	}

	public String getPath() {
		return path;
	}

	public ListCmd views() {
		this.type = ListType.VIEWS;
		return this;
	}
	
	public ListCmd clusters() {
		this.type = ListType.CLUSTERS;
		return this;
	}
	
	public ListCmd tables(String cluster) {
		
		if (cluster == null) {
			throw new IllegalArgumentException("empty cluster");
		}
		
		this.path = cluster;
		this.type = ListType.TABLES;
		return this;
	}
	
	private ListOperation buildRequest() {
		
		if (type == null) {
			throw new IllegalArgumentException("type is null");
		}
		
		ListOperation.Builder builder = ListOperation.newBuilder();
		
		builder.setType(type);
		
		if (path != null) {
			builder.setPath(path);
		}
		
		return builder.build();
	}
	
	public List<Entry> sync() {
		
		try {
			return doSync();
		}
		catch(RuntimeException e) {
			throw new GkvsException("sync fail " + this, e);
		}
	}
	
	private List<Entry> doSync() {
		
		ListResult result = instance.getBlockingStub().list(buildRequest());
		
		return Transformers.toEntryList(result);
		
	}
	
	public GkvsFuture<List<Entry>> async() {
		
		ListenableFuture<ListResult> result = instance.getFutureStub().list(buildRequest());
		
		return new GkvsFuture<List<Entry>>(Transformers.toEntryList(result));
		
	}

	@Override
	public String toString() {
		return "ListCmd [type=" + type + ", path=" + path + "]";
	}

}
