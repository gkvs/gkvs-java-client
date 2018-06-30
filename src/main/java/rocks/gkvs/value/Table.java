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

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.msgpack.core.MessagePacker;
import org.msgpack.value.impl.ImmutableArrayValueImpl;
import org.msgpack.value.impl.ImmutableLongValueImpl;
import org.msgpack.value.impl.ImmutableMapValueImpl;
import org.msgpack.value.impl.ImmutableNilValueImpl;
import org.msgpack.value.impl.ImmutableStringValueImpl;

/**
 * 
 * Table
 *
 * @author Alex Shvid
 * @date Jun 27, 2018 
 *
 */

public final class Table extends Value {

	interface InternalTable {
		
		TableType type();
		
		Value get(String key);
		
		Value get(int key);
		
		Value put(String key, Value value);
		
		Value put(int key, Value value);
		
		Value remove(String key);
		
		Value remove(int key);
		
		Set<String> keySet();
		
		int[] sortedKeys();
		
		int firstKey();
		
		int lastKey();
		
		int size();
		
		void clear();
		
		String asString();
		
		org.msgpack.value.Value toMsgpackValue(); 
		
		void writeTo(MessagePacker packer) throws IOException;
		
		void print(StringBuilder str, int initialSpaces, int tabSpaces);
		
	}
	
	enum NullTable implements InternalTable {
		
		NULL;
		
		public TableType type() {
			return TableType.EMPTY;
		}
		
		public Value get(String key) {
			return Nil.get();
		}
		
		public Value get(int key) {
			return Nil.get();
		}
		
		public Value put(String key, Value value) {
			throw new UnsupportedOperationException("put in to immutable NULL table");
		}
		
		public Value put(int key, Value value) {
			throw new UnsupportedOperationException("put in to immutable NULL table");
		}
		
		public Value remove(String key) {
			throw new UnsupportedOperationException("remove in immutable NULL table");
		}
		
		public Value remove(int key) {
			throw new UnsupportedOperationException("remove in immutable NULL table");
		}
		
		public Set<String> keySet() {
			return Collections.emptySet();
		}
		
		public int[] sortedKeys() {
			return new int[0];
		}
		
		public int firstKey() {
			return 0;
		}
		
		public int lastKey() {
			return -1;
		}
		
		public int size() {
			return 0;
		}
		
		public void clear() {
		}
		
		public String asString() {
			return "{}";
		}
		
		public org.msgpack.value.Value toMsgpackValue() {
			throw new UnsupportedOperationException("serialize immutable NULL table");
		}
		
		public void writeTo(MessagePacker packer) throws IOException {
			throw new UnsupportedOperationException("serialize immutable NULL table");
		}
		
		public void print(StringBuilder str, int initialSpaces, int tabSpaces) {
		}
		
	}
	
	final class EmptyTable implements InternalTable {
				
		private EmptyTable() {
		}
		
		public TableType type() {
			return TableType.EMPTY;
		}
		
		public Value get(String key) {
			return Nil.get();
		}
		
		public Value get(int key) {
			return Nil.get();
		}
		
		public Value put(String key, Value value) {
			
			int ikey;
			try {
				ikey = Integer.parseInt(key);
			}
			catch(NumberFormatException e) {
				return switchToMap().put(key, value);
			}
			
			return switchToList().put(ikey, value);
		}
		
		public Value put(int key, Value value) {
			return switchToList().put(key, value);
		}
		
		public Value remove(String key) {
			return Nil.get();
		}
		
		public Value remove(int key) {
			return Nil.get();
		}
		
		public Set<String> keySet() {
			return Collections.emptySet();
		}
		
		public int[] sortedKeys() {
			return new int[0];
		}
		
		public int firstKey() {
			return 0;
		}
		
		public int lastKey() {
			return -1;
		}
		
		public int size() {
			return 0;
		}
		
		public void clear() {
		}
		
		public String asString() {
			return "{}";
		}
		
		public org.msgpack.value.Value toMsgpackValue() {
			return ImmutableNilValueImpl.get();
		}
		
		public void writeTo(MessagePacker packer) throws IOException {
			packer.packArrayHeader(0);
		}
		
		public void print(StringBuilder str, int initialSpaces, int tabSpaces) {
		}
		
	}
	
	final class ListTable implements InternalTable {

		private final SparseList<Value> list = new SparseList<Value>();

		public TableType type() {
			return TableType.LIST;
		}

		public SparseList<Value> getList() {
			return list;
		}
		
		public Value get(String key) {
			return list.get(Integer.parseInt(key));
		}
		
		public Value get(int key) {
			return list.get(key);
		}
		
		public Value put(String key, Value value) {

			int ikey;
			try {
				ikey = Integer.parseInt(key);
			}
			catch(NumberFormatException e) {
				return switchToMap().put(key, value);
			}

			return put(ikey, value); 
		}
		
		public Value put(int key, Value value) {
			
			if (value != null) {
				return list.set(key, value);
			}
			else {
				return list.remove(key);
			}
			
		}
		
		public Value remove(String key) {
			
			int ikey;
			try {
				ikey = Integer.parseInt(key);
			}
			catch(NumberFormatException e) {
				// suppress exception
				return Nil.get();
			}
			
			return list.remove(ikey);
			
		}
		
		public Value remove(int key) {
			return list.remove(key);
		}
		
		public Set<String> keySet() {
			Set<String> set = new HashSet<>();
			int sz = list.size();
			for (int i = 0; i != sz; ++i) {
				set.add(Integer.toString(list.keyAt(i)));
			}
			return set;
		}
		
		public int[] sortedKeys() {
			int sz = list.size();
			int[] out = new int[sz];
			for (int i = 0; i < sz; ++i) {
				out[i] = list.keyAt(i);
			}
			return out;
		}
		
		public int firstKey() {
			return list.firstKey();
		}
		
		public int lastKey() {
			return list.lastKey();
		}
		
		public int size() {
			return list.size();
		}
		
		public void clear() {
			list.clear();
		}
		
		public String asString() {
			return list.toString();
		}
		
		public org.msgpack.value.Value toMsgpackValue() {
			
			if (list.isSequence()) {
				return toArray();
			}
			else {
				return toIntMap();
			}
			
		}
		
		private org.msgpack.value.Value toArray() {
			
			int size = size();
			org.msgpack.value.Value[] array = new org.msgpack.value.Value[size];
			
			int index = 0;
			for (int i = 0; i < size; ++i) {
				
				Value value = list.valueAt(i);
				array[index++] = value.toMsgpackValue();
			}
			
			return new ImmutableArrayValueImpl(array);
		}
		
		private org.msgpack.value.Value toIntMap() {
			
			int size = list.size();

			int capacity = size << 1;
			org.msgpack.value.Value[] array = new org.msgpack.value.Value[capacity];

			int index = 0;
			for (int i = 0; i < size; ++i) {
				
				int key = list.keyAt(i);
				Value value = list.valueAt(i);
				
				array[index++] = new ImmutableLongValueImpl(key);
				array[index++] = value.toMsgpackValue();
				
			}

			return new ImmutableMapValueImpl(array);
			
		}
		
		public void writeTo(MessagePacker packer) throws IOException {
			
			if (list.isSequence()) {
				writeArrayTo(packer);
			}
			else {
				writeIntMapTo(packer);
			}
			
		}
		
		private void writeArrayTo(MessagePacker packer) throws IOException {
			
			int size = list.size();
			packer.packArrayHeader(size);
			
			for (int i = 0; i < size; ++i) {

				Value value = list.valueAt(i);
				value.writeTo(packer);
			}
			
		}
		
		private void writeIntMapTo(MessagePacker packer) throws IOException {

			int size = list.size();
			packer.packMapHeader(size);

			for (int i = 0; i < size; ++i) {

				int key = list.keyAt(i);
				Value value = list.valueAt(i);

				packer.packInt(key);
				value.writeTo(packer);
			}

		}
		
		public void print(StringBuilder str, int initialSpaces, int tabSpaces) {
			boolean first = true;
			int size = list.size();
			for (int i = 0; i < size; ++i) {
				if (!first) {
					str.append(",\n");
				}
				addSpaces(str, initialSpaces + tabSpaces);
				int key = list.keyAt(i);
				Value value = list.valueAt(i);
				str.append(key).append("=");
				value.print(str, initialSpaces + tabSpaces, tabSpaces);
				first = false;
			}
			str.append("\n");
		}	
		
	}

	final class MapTable implements InternalTable {

		private final Map<String, Value> map = new HashMap<String, Value>();

		public TableType type() {
			return TableType.MAP;
		}
		
		MapTable() {
		}
		
		MapTable(SparseList<Value> list) {
			
			int size = list.size();
			
			for (int i = 0; i != size; ++i) {
				
				int key = list.keyAt(i);
				Value value = list.valueAt(i);
				
				map.put(Integer.toString(key), value);
				
			}
			
		}

		public Map<String, Value> getMap() {
			return map;
		}
		
		public Value get(String key) {
			return map.get(key);
		}
		
		public Value get(int key) {
			return map.get(Integer.toString(key));
		}
		
		public Value put(String key, Value value) {

			if (value != null) {
				return map.put(key, value);
			}
			else {
				return map.remove(key);
			}
			
		}
		
		public Value put(int key, Value value) {
			return put(Integer.toString(key), value);
		}
		
		public Value remove(String key) {
			return map.remove(key);
		}
		
		public Value remove(int key) {
			return map.remove(Integer.toString(key));
		}
		
		public Set<String> keySet() {
			return map.keySet();
		}
		
		public int[] sortedKeys() {
			int[] arr = new int[map.size()];
			
			int i = 0;
			for (String key : map.keySet()) {

				try {
					arr[i++] = Integer.parseInt(key);
				} catch (NumberFormatException e) {
					// Suppress exception
				}
			}

			if (i < map.size()) {
				arr = Arrays.copyOf(arr, i);
			}
			
			Arrays.sort(arr);
			return arr;
		}
		
		public int firstKey() {
			
			Integer minKey = null;

			for (String key : map.keySet()) {

				try {
					int value = Integer.parseInt(key);
					if (minKey == null || value < minKey.intValue()) {
						minKey = value;
					}
				} catch (NumberFormatException e) {
					// ignore
				}
			}

			return minKey != null ? minKey.intValue() : -1;
			
		}
		
		public int lastKey() {
			
			Integer maxKey = null;

			for (String key : map.keySet()) {

				try {
					int value = Integer.parseInt(key);
					if (maxKey == null || value > maxKey.intValue()) {
						maxKey = value;
					}
				} catch (NumberFormatException e) {
					// ignore
				}
			}

			return maxKey != null ? maxKey.intValue() : -1;
		}
		
		public int size()  {
			return map.size();
		}
		
		public void clear() {
			map.clear();
		}
		
		public String asString() {
			StringBuilder str = new StringBuilder();
			str.append("{");
			boolean first = true;
			for (Map.Entry<String, Value> entry : map.entrySet()) {
				if (!first) {
					str.append(", ");
				}
				str.append(entry.getKey()).append("=").append(entry.getValue().asString());
				first = false;
			}
			str.append("}");
			return str.toString();
		}
		
		public org.msgpack.value.Value toMsgpackValue() {

			int size = map.size();

			int capacity = size << 1;
			org.msgpack.value.Value[] array = new org.msgpack.value.Value[capacity];

			int index = 0;
			for (Map.Entry<String, Value> entry : map.entrySet()) {

				Value val = entry.getValue();

				array[index++] = new ImmutableStringValueImpl(entry.getKey());
				array[index++] = val.toMsgpackValue();

			}

			return new ImmutableMapValueImpl(array);
		}
		
		public void writeTo(MessagePacker packer) throws IOException {
			
			int size = map.size();

			packer.packMapHeader(size);

			for (Map.Entry<String, Value> entry : map.entrySet()) {

				String key = entry.getKey();
				Value value = entry.getValue();

				byte[] data = key.getBytes(StandardCharsets.UTF_8);
				packer.packRawStringHeader(data.length);
				packer.writePayload(data);

				value.writeTo(packer);
			}

			
		}

		public void print(StringBuilder str, int initialSpaces, int tabSpaces) {
			boolean first = true;
			for (Map.Entry<String, Value> entry : map.entrySet()) {
				if (!first) {
					str.append(",\n");
				}
				addSpaces(str, initialSpaces + tabSpaces);
				str.append(entry.getKey()).append("=");
				entry.getValue().print(str, initialSpaces + tabSpaces, tabSpaces);
				first = false;
			}
			str.append("\n");
		}		
	}

	private InternalTable switchToList() {
		
		if (table.type() == TableType.EMPTY) {
			table = new ListTable();
		}
		
		return table;
	}
	
	private InternalTable switchToMap() {
		
		if (table.type() == TableType.LIST) {
			table = new MapTable(((ListTable)table).getList());
		}
		else {
			table = new MapTable();
		}

		return table;
	}
	
	public static final Table NULL = new Table(NullTable.NULL);
	
	private InternalTable table;
	
	public Table() {
		this.table = new EmptyTable();
	}
	
	private Table(InternalTable internalTable) {
		this.table = internalTable;
	}
	
	public TableType getType() {
		return table.type();
	}
	
	@Override
	public boolean isNil() {
		return table == NullTable.NULL;
	}

	@Override
	public Bool asBool(Bool defaultValue) {
		return defaultValue;
	}
	
	@Override
	public Num asNum(Num defaultValue) {
		return defaultValue;
	}
	
	@Override
	public Str asStr(Str defaultValue) {
		return defaultValue;
	}
	
	@Override
	public Table asTable(Table defaultTable) {
		return this;
	}

	public Value get(String key) {

		if (key == null) {
			throw new IllegalArgumentException("key is null");
		}

		return table.get(key);
	}

	public Value get(int key) {
		return table.get(key);
	}

	public Value get(Field field) {

		if (field == null) {
			throw new IllegalArgumentException("field is null");
		}

		if (field.isEmpty()) {
			return this;
		}

		int lastIndex = field.size() - 1;
		Table currentTable = navigateTable(field, false);

		if (currentTable != null) {
			String key = field.get(lastIndex);
			return currentTable.get(key);
		}
		
		return null;

	}
	
	public Value put(String key, Value value) {

		if (key == null) {
			throw new IllegalArgumentException("key is null");
		}

		return table.put(key, value);
	}

	public Value put(String key, String stringfyValue) {
		return put(key, Parser.parseStringifyValue(stringfyValue));
	}

	public Value put(int key, Value value) {
		return table.put(key, value);
	}

	public Value put(int key, String stringfyValue) {
		return put(key, Parser.parseStringifyValue(stringfyValue));
	}

	public Value put(Field field, Value value) {

		if (field == null) {
			throw new IllegalArgumentException("field is null");
		}

		if (field.isEmpty()) {
			return null;
		}

		Table currentTable = navigateTable(field, true);
		String key = field.get(field.size() - 1);
		if (value != null) {
			return currentTable.put(key, value);
		}
		else {
			return currentTable.remove(key);
		}
	}

	public Value put(Field field, String stringfyValue) {
		return put(field, Parser.parseStringifyValue(stringfyValue));
	}

	public Value remove(String key) {
		
		if (key == null) {
			throw new IllegalArgumentException("key is null");
		}
		
		return table.remove(key);
	}

	public Value remove(int key) {
		return table.remove(key);
	}

	public Value remove(Field field) {
		
		if (field == null) {
			throw new IllegalArgumentException("null field");
		}

		if (field.isEmpty()) {
			return null;
		}

		Table currentTable = navigateTable(field, true);
		String key = field.get(field.size() - 1);
		return currentTable.remove(key);
	}

	private Table navigateTable(Field field, boolean create) {
		
		int lastIndex = field.size() - 1;
		
		Table currentTable = this;
		for (int i = 0; i != lastIndex; ++i) {

			String key = field.get(i);
			Value existingValue = currentTable.get(key);

			if (existingValue == null || !(existingValue instanceof Table)) {
				if (create) {
					Table newTable = new Table();
					currentTable.put(key, newTable);
					currentTable = newTable;
				}
				else {
					return null;
				}
			} else {
				currentTable = (Table) existingValue;
			}

		}
		return currentTable;
	}
	
	public Set<String> keySet() {
		return table.keySet();
	}

	public int[] sortedKeys() {
		return table.sortedKeys();
	}

	public int firstKey() {
		return table.firstKey();
	}
	
	public int lastKey() {
		return table.lastKey();
	}

	public int size() {
		return table.size();
	}

	public void clear() {
		table.clear();
	}

	@Override
	public String asString() {
		return table.asString();
	}

	@Override
	public org.msgpack.value.Value toMsgpackValue() {
		return table.toMsgpackValue();
	}

	@Override
	public void writeTo(MessagePacker packer) throws IOException {
		table.writeTo(packer);
	}

	@Override
	public void print(StringBuilder str, int initialSpaces, int tabSpaces) {
		str.append("Table [type=" + table.type() + ", size=" + table.size() + "] {\n");
		table.print(str, initialSpaces, tabSpaces);
		addSpaces(str, initialSpaces);
		str.append("}");
	}

	private static void addSpaces(StringBuilder str, int spaces) {
		for (int i = 0; i != spaces; ++i) {
			str.append(' ');
		}
	}

}
